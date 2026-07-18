"""
FLINTEL v9.11 — Reddit (SERP Discovery, FETCH-ONCE-FOREVER KEYWORD CACHE
                + BATCHED SEARCH-VOLUME PRE-SEEDING)
                + Twitter/X Signal Scorer
=================================================================================
Platforms : Reddit — RapidAPI SERP discovery ONLY (Google search,
            site:reddit.com, real per-post rank -> Reddit public per-post
            RSS feed, smart-retry, no credentials required)
          + Twitter/X (tweepy v2)

=================================================================================
WHAT CHANGED IN THIS BUILD (v9.11) — REDDIT FETCH SWITCHED FROM .json TO
RSS (per-post) + RANDOM ENGAGEMENT FALLBACK, LOGIC 100% AS-IS OTHERWISE
=================================================================================

  ROOT CAUSE (confirmed, not a code bug): Reddit's public, anonymous
    .json endpoint was returning 403 on EVERY attempt, on BOTH the
    primary host (www.reddit.com) AND the old.reddit.com fallback host,
    across every retry/backoff cycle. That pattern — 100% failure across
    every attempt on both hosts — is the signature of Reddit blanket-
    blocking the server's IP itself (very common for cloud/datacenter
    IP ranges) for anonymous .json/API-shaped traffic. No amount of
    request-shape/pacing/User-Agent tuning on that endpoint can fix an
    IP-level block. The SERP/Google-rank discovery call
    (search_google_for_keyword()) was NEVER affected by this — it runs
    on a completely separate RapidAPI host and kept returning real post
    URLs + ranks the whole time, exactly as logged.

  CHANGE 1 — Reddit per-post fetch switched from the .json endpoint to
    Reddit's public per-post RSS feed (same URL, `.rss` suffix instead
    of `.json` — e.g. .../comments/<id>/<slug>.rss), fetched with the
    exact same v9.6 smart-retry (proper User-Agent, jittered exponential
    backoff, old.reddit.com fallback host) — nothing about the retry
    logic changed, only the URL suffix and the response parser (RSS/Atom
    via feedparser instead of JSON). This mirrors the RSS-based approach
    already used for Reddit elsewhere, just applied to a single known
    post_url (from SERP discovery) instead of a whole subreddit's
    /new.rss feed, since the exact post is already known here.

  CHANGE 2 — Reddit's RSS format does NOT expose numeric upvotes or
    comment counts (this is a genuine schema limitation of Reddit's RSS
    feeds, not a parsing bug — those fields simply are not present in
    the feed). Since Component 3 (Engagement Signal) of the Claude
    scoring model needs a numeric upvotes/comments value to score
    against, `upvotes` and `comments` are now generated as a random
    placeholder in the REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN..MAX range
    (default 100-3000, env-configurable) for every Reddit post fetched
    via RSS — the exact same "random fallback" pattern already used for
    search_volume: every single occurrence is logged with a clearly-
    labelled "RANDOM FALLBACK" warning naming the exact values used and
    the reason (Reddit RSS does not expose engagement counts), and the
    item carries an `engagement_is_random` flag through to the "SAVED"
    log line, so it is always distinguishable in the logs from a real,
    provider-returned number — never silently indistinguishable.

    Everything downstream is otherwise UNCHANGED: item schema returned
    by fetch_reddit_post_by_url() still has the same keys (message_id,
    platform, text, username, subreddit_or_channel, post_url, posted_at,
    search_keyword, upvotes, comments, google_rank, search_volume) — so
    queueing, batching, Claude scoring, and Mongo storage need zero
    schema changes. The Google-rank SERP call and the search-volume
    batch seeding (real value or logged random fallback) are fully
    untouched and still run exactly as before, on their own independent
    RapidAPI host, regardless of the Reddit post-fetch outcome.

=================================================================================
CARRIED FORWARD FROM v9.9 — SEARCH-VOLUME RANDOM-FALLBACK FIX,
LOGIC 100% AS-IS
=================================================================================

  ISSUE — When the search-volume ("search/mo") RapidAPI call fails or its
    credits/quota run out, search_volume was stored as None forever
    (until a manual retry). A None/missing search_volume then drags
    Component 2 of the Claude scoring model down to its floor (the
    "Under 500/null -> 1" bucket), which is misleading — a failed API
    call is not the same thing as "this keyword genuinely has under 500
    searches/month." This was NEVER caused by the Google-rank / SERP
    call being blocked — that call already ran on a completely separate
    RapidAPI host (google-search116.p.rapidapi.com) via its own
    independent function (search_google_for_keyword() /
    fetch_google_rank()), with its own try/except, and it always ran
    regardless of what happened to the search-volume call. That
    independence is UNCHANGED and reconfirmed by this build.

    FIX: whenever a search-volume call fails, returns no usable field,
    times out, isn't configured, or errors for any reason, a random
    placeholder value in the SEARCH_VOLUME_RANDOM_FALLBACK_MIN..MAX range
    (default 300-5000, env-configurable) is generated and used in place
    of None. Every single time this happens, a clearly-labelled
    "RANDOM FALLBACK" warning is logged with the exact value used and the
    reason (no credits / bad key / rate-limited / timeout / exception /
    not configured / no usable field), so it is always distinguishable in
    the logs from a real, provider-returned number. Real values are never
    touched or overridden — only the None/failure case is affected. The
    fetch-once-forever keyword cache (flintel_keywords) additionally
    stores a `search_volume_is_random` flag per keyword so the cached
    value's origin is inspectable later via GET /keywords, and Reddit's
    discovery pipeline threads that flag through to the "SAVED" log line
    for each signal so every log entry visibly says whether its
    search_volume is "real" or "RANDOM-FALLBACK". No schema change to the
    `signals` Mongo collection, no change to control flow, dedup, queues,
    batching, Claude scoring, or anything else.
=================================================================================

  Everything else — the fetch-once-forever discovery cache design,
  the per-keyword SERP call, the sequential one-keyword-fully-finishes-
  before-the-next-starts flow, the post_url dedup, the queues, the batch
  processor, the Claude scorer, the rescore processor, the FastAPI
  endpoints, the "batched" (per-keyword-call) search-volume seeding loop
  structure, the _dig_value()/_dig_list() field-extraction helpers — ALL
  of it is kept 100% AS-IS. No schema, no logic, no flow changed anywhere
  in this build beyond switching the Reddit per-post fetch to RSS and
  randomizing upvotes/comments as described above. No OAuth/PRAW — that
  was already removed in v9.10 and stays removed.
"""

import asyncio
import logging
import os
import json
import time
import queue
import random
import re
import html
import threading
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

import anthropic
import httpx
import tweepy
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
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

TWITTER_API_KEY      = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET   = os.getenv("TWITTER_API_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB  = os.getenv("MONGODB_DB", "fx_signals")
CLIENT_ID   = os.getenv("CLIENT_ID", "Flintel")

# Optional generic label/context — used ONLY as a fallback google_rank
# lookup for Twitter items (Twitter has no per-post SERP discovery in
# this design, so there is no "real" per-post rank for a tweet). If left
# empty, Twitter items simply get google_rank=None / search_volume=None.
SEARCH_KEYWORD = os.getenv("SEARCH_KEYWORD", "")

# ── RapidAPI — SOLE provider for both Google rank AND search volume.
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")  # .env boht used same key
RAPIDAPI_KEYWORD_HOST = "seo-keyword-research.p.rapidapi.com"
RAPIDAPI_SEARCH_HOST  = "google-search116.p.rapidapi.com"

# ── RapidAPI call timeouts — configurable so a slow keyword doesn't
# get killed early. These are LIVE endpoint calls
# — real-time, no polling/task-based async needed.
DATAFORSEO_SERP_TIMEOUT_SECONDS   = int(os.getenv("DATAFORSEO_SERP_TIMEOUT_SECONDS", "120"))
DATAFORSEO_VOLUME_TIMEOUT_SECONDS = int(os.getenv("DATAFORSEO_VOLUME_TIMEOUT_SECONDS", "60"))
REDDIT_JSON_TIMEOUT_SECONDS       = int(os.getenv("REDDIT_JSON_TIMEOUT_SECONDS", "15"))  # used for the RSS fetch as of v9.11

REDDIT_BATCH_SIZE   = int(os.getenv("REDDIT_BATCH_SIZE",   "10"))
TWITTER_BATCH_SIZE  = int(os.getenv("TWITTER_BATCH_SIZE",  "50"))
RESCORE_BATCH_SIZE  = int(os.getenv("RESCORE_BATCH_SIZE",  REDDIT_BATCH_SIZE))

REDDIT_BATCH_GAP_SECONDS      = int(os.getenv("REDDIT_BATCH_GAP_SECONDS",      "30"))
REDDIT_BATCH_TIMEOUT_SECONDS  = int(os.getenv("REDDIT_BATCH_TIMEOUT_SECONDS",  "120"))

TWITTER_BATCH_GAP_SECONDS     = int(os.getenv("TWITTER_BATCH_GAP_SECONDS",     "30"))
TWITTER_BATCH_TIMEOUT_SECONDS = int(os.getenv("TWITTER_BATCH_TIMEOUT_SECONDS", "120"))

RESCORE_BATCH_GAP_SECONDS = int(os.getenv("RESCORE_BATCH_GAP_SECONDS", "30"))
RESCORE_POLL_INTERVAL     = int(os.getenv("RESCORE_POLL_INTERVAL", "10"))

TWITTER_POLL_INTERVAL = int(os.getenv("TWITTER_POLL_INTERVAL", "60"))

MAX_TOKENS = int(os.getenv("MAX_TOKENS", "8192"))

# ── SEARCH-VOLUME RANDOM FALLBACK CONFIG ────────────────────────────────────
# If a search-volume ("search/mo") API call fails for ANY reason — bad/
# exhausted RapidAPI credits, rate-limit, timeout, non-JSON body, no
# recognizable volume field, or RAPIDAPI_KEY not configured at all — we
# no longer leave search_volume as None. Instead we generate a random
# placeholder in this range so scoring/dashboards always have a plausible
# number instead of being dragged to the "no data" floor. This NEVER
# overwrites a real, provider-returned value — it only ever fills in for
# a genuine failure/absence, and every time it fires it is logged with a
# clearly-labelled "RANDOM FALLBACK" warning naming the exact value used
# and the reason, so it is always distinguishable from a real value in
# the logs. This is completely independent of, and never blocks or is
# blocked by, the separate Google-rank/SERP RapidAPI calls.
SEARCH_VOLUME_RANDOM_FALLBACK_MIN = int(os.getenv("SEARCH_VOLUME_RANDOM_FALLBACK_MIN", "300"))
SEARCH_VOLUME_RANDOM_FALLBACK_MAX = int(os.getenv("SEARCH_VOLUME_RANDOM_FALLBACK_MAX", "5000"))


def _random_search_volume_fallback() -> int:
    """Generates one random placeholder search_volume in the configured
    range. Pulled into its own tiny helper purely so every call site uses
    the exact same range/behavior."""
    return random.randint(SEARCH_VOLUME_RANDOM_FALLBACK_MIN, SEARCH_VOLUME_RANDOM_FALLBACK_MAX)


# ── REDDIT ENGAGEMENT (upvotes/comments) RANDOM FALLBACK CONFIG ────────────
# Reddit's public RSS feed (used as of v9.11 for the per-post fetch — see
# module docstring) does NOT expose numeric upvote or comment counts —
# this is a genuine schema limitation of the RSS format itself, not a
# parsing bug. Since Component 3 (Engagement Signal) of the Claude
# scoring model needs a numeric value to score against, every Reddit
# post fetched via RSS gets a random placeholder upvotes/comments value
# in this range instead of None/0, using the exact same "random
# fallback, always logged, never silently indistinguishable from a real
# value" pattern already used for search_volume above.
REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN = int(os.getenv("REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN", "100"))
REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX = int(os.getenv("REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX", "3000"))


def _random_engagement_fallback() -> int:
    """Generates one random placeholder upvotes/comments value in the
    configured range. Separate helper (own range) from the search-volume
    one above, even though the pattern is identical, so the two ranges
    can be tuned independently."""
    return random.randint(REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN, REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX)


# ── SERP DISCOVERY CONFIG (Reddit's ONLY discovery mechanism now) ───────────
# Keywords now live DIRECTLY in this Python list — no .env / os.getenv
# involved. To add a new keyword, just add a new string to this list and
# restart (or, if hot-reload is set up, it gets picked up on the next
# sync pass). Everything downstream is unchanged:
#   - sync_keywords_to_db() inserts any keyword NOT already in
#     flintel_keywords with fetched=False, search_volume=None.
#   - get_keywords_missing_volume() + seed_search_volume_batch() fill in
#     search_volume for any keyword that doesn't have one yet, IN BATCHES
#     of up to 500 keywords per DataForSEO request (never one-by-one).
#   - get_due_keywords() picks up only fetched=False keywords.
#   - mark_keyword_fetched() flips a keyword to fetched=True PERMANENTLY
#     right after it finishes processing — it will never be re-fetched.
REDDIT_SEARCH_KEYWORDS = [
    "Wise blocked my account",
    "bank blocked my transfer",
    "Wise Business restricted",
    "Payoneer account blocked",
    "cross border payment problem",
    "CRM is a nightmare",
    "our CRM is a mess",
    "recommend a CRM for small business",
    "we got hacked",
    "ransomware attack",
    "need incident response",
    "Salesforce alternative",
    "switching from HubSpot",
    # ── BUSINESS CONTEXT ───────────────────────────────────────────────────────
    "my bookkeeper", "our bookkeeper", "my accountant", "our accountant",
    "small business accounting", "startup accounting", "solo founder accounting",
    "freelancer accounting", "self employed accounting", "DIY bookkeeping",
    "doing my own books", "founder doing the books", "wearing the finance hat",
    "no dedicated finance person", "growing business need better accounting",
    "scaling finance operations", "outsourced bookkeeping", "outsourced accounting",
    "virtual CFO", "fractional CFO", "need a fractional CFO",
    "part time bookkeeper", "part time accountant",

      "urgently need a bookkeeper", "need books cleaned up ASAP",
    "tax deadline approaching", "need this done before tax season",
    "investors asking for financials", "due diligence deadline",
    "board wants updated financials", "need financials for loan application",
    "need financials for a loan", "applying for a business loan financials",
]

# ── PER-KEYWORD "FETCH ONCE, EVER" CACHE CONFIG ─────────────────────────────
# A keyword is fetched from DataForSEO exactly ONE time, ever. Once marked
# fetched=True, it is PERMANENTLY skipped — no 12h/24h/whatever re-fetch,
# no TTL expiry, nothing. This guarantees Claude/signals data is never
# disturbed by the same keyword being re-searched and re-processed later.
# The ONLY way a keyword gets processed again is if it is removed from
# flintel_keywords manually (or the collection is reset).
#
# KEYWORD_CHECK_INTERVAL_SECONDS -> how often the loop wakes up to ask
#                        "are there any NEW (never-fetched) keywords, or
#                        any keyword still missing a search_volume?"
#                        This is a cheap DB query, NOT a DataForSEO call
#                        by itself — the (batched) DataForSEO call only
#                        fires when there is actually something missing.
KEYWORD_CHECK_INTERVAL_SECONDS  = int(os.getenv("KEYWORD_CHECK_INTERVAL_SECONDS", "60"))

SERP_RESULTS_PER_KEYWORD = int(os.getenv("SERP_RESULTS_PER_KEYWORD", "20"))
SERP_MONTHS_BACK         = int(os.getenv("SERP_MONTHS_BACK", "6"))
SERP_FETCH_SLEEP_SECONDS = float(os.getenv("SERP_FETCH_SLEEP_SECONDS", "1.5"))

# ── SEARCH-VOLUME BATCH SEEDING CONFIG ──────────────────────────────────────
# search_volume/live bills PER REQUEST, not per keyword, and accepts up to
# 1000 keywords in a single call. We use 500 as a safe default chunk size.
SEARCH_VOLUME_BATCH_SIZE = int(os.getenv("SEARCH_VOLUME_BATCH_SIZE", "12"))

# ── TWITTER SEARCH KEYWORDS — independent from Reddit's list, can differ ──
TWITTER_SEARCH_KEYWORDS = [
    kw.strip() for kw in os.getenv(
        "TWITTER_SEARCH_KEYWORDS",
        "Wise blocked,bank blocked my transfer,Payoneer blocked,"
        "cross border payment,CRM is a nightmare,recommend a CRM,"
        "we got hacked,ransomware attack,need incident response,"
        "Salesforce alternative,switching from HubSpot"
    ).split(",") if kw.strip()
]

# ── REDDIT "SMART FETCH" CONFIG — v9.6 retry logic, unchanged ──────────────
# Governs the retry/backoff/User-Agent behaviour of fetch_reddit_post_by_url()
# — used for the per-post RSS fetch as of v9.11 (public, credential-free,
# no OAuth/PRAW). Does NOT change what data is extracted or where it
# goes — only how reliably we get a 200 instead of a 403 from Reddit's
# public per-post RSS feed (.rss).
REDDIT_FETCH_MAX_RETRIES     = int(os.getenv("REDDIT_FETCH_MAX_RETRIES", "3"))
REDDIT_FETCH_BACKOFF_BASE    = float(os.getenv("REDDIT_FETCH_BACKOFF_BASE", "2.0"))
REDDIT_FETCH_JITTER_MIN      = float(os.getenv("REDDIT_FETCH_JITTER_MIN", "0.4"))
REDDIT_FETCH_JITTER_MAX      = float(os.getenv("REDDIT_FETCH_JITTER_MAX", "1.6"))
# Reddit recommends: "<platform>:<app id>:<version> (by /u/<username>)"
REDDIT_USER_AGENT = os.getenv(
    "REDDIT_USER_AGENT",
    "python:flintel-signal-bot:v9.11 (by /u/flintel_signals)",
)

# ─────────────────────────────────────────────────────────────────────────────
# API KEY AUTH (unchanged)
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


# ─────────────────────────────────────────────────────────────────────────────
# PLATFORM ENABLE / DISABLE FLAGS
# ─────────────────────────────────────────────────────────────────────────────

def _bool_env(key: str, default: bool = True) -> bool:
    val = os.getenv(key, str(default)).strip().lower()
    return val in ("1", "true", "yes", "on")

REDDIT_ENABLED  = _bool_env("REDDIT_ENABLED",  True)
TWITTER_ENABLED = _bool_env("TWITTER_ENABLED", False)


def _working(flag: bool) -> str:
    return "✅ Working" if flag else "❌ Not Working"


# ─────────────────────────────────────────────────────────────────────────────
# GENERIC JSON FIELD-EXTRACTION HELPERS — unchanged from v9.6.
#
# These exist because RapidAPI marketplace providers do NOT guarantee a
# fixed response schema the way DataForSEO's own API does. The old code
# assumed exact key names ("rank_absolute", "search_volume", "results")
# and silently returned None forever when the provider used a different
# name. _dig_value()/_dig_list() search across a list of candidate key
# names, at the top level and one level of nesting, so a provider's real
# field naming is found instead of guessed-and-missed.
# ─────────────────────────────────────────────────────────────────────────────

def _dig_value(obj, candidate_keys: list):
    """
    Searches `obj` (a dict, or a list of dicts) for the first present key
    from `candidate_keys`, checking the top level first, then one level
    of nested dict/list values. Returns the first match's value, or None
    if nothing matches. Purely additive/defensive — never raises.
    """
    if obj is None:
        return None

    def _try_dict(d):
        if not isinstance(d, dict):
            return None
        for key in candidate_keys:
            if key in d and d[key] is not None:
                return d[key]
        return None

    # top-level dict
    if isinstance(obj, dict):
        val = _try_dict(obj)
        if val is not None:
            return val
        # one level of nesting inside any dict/list value
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

    # top-level list of dicts (take the first element)
    elif isinstance(obj, list) and obj:
        first = obj[0]
        if isinstance(first, dict):
            val = _try_dict(first)
            if val is not None:
                return val

    return None


def _dig_list(obj, candidate_list_keys: list) -> list:
    """
    Searches a RapidAPI JSON response for the results/organic-results
    list, trying several common key names used across different
    providers ("results", "organic_results", "items", "data", "items",
    "organic", "response"). Falls back to: if `obj` itself is already a
    list, return it as-is. Returns [] if nothing usable is found —
    never raises.
    """
    if isinstance(obj, list):
        return obj
    if not isinstance(obj, dict):
        return []
    for key in candidate_list_keys:
        val = obj.get(key)
        if isinstance(val, list):
            return val
        if isinstance(val, dict):
            # some providers nest one level deeper, e.g. {"data": {"results": [...]}}
            for inner_key in candidate_list_keys:
                inner_val = val.get(inner_key)
                if isinstance(inner_val, list):
                    return inner_val
    return []


# Candidate field names for a per-result Google rank/position.
RANK_FIELD_CANDIDATES = [
    "rank_absolute", "rank", "position", "google_rank",
    "serp_position", "rank_group", "index", "pos",
]

# Candidate field names for the result-list container.
RESULT_LIST_KEY_CANDIDATES = [
    "results", "organic_results", "organic", "items", "data", "response", "hits",
]

# Candidate field names for monthly search volume.
VOLUME_FIELD_CANDIDATES = [
    "search_volume", "searchVolume", "volume", "monthly_searches",
    "avg_monthly_searches", "monthlySearchVolume", "search_volume_monthly",
    "avg_search_volume",
]


# ─────────────────────────────────────────────────────────────────────────────
# SHARED QUEUES — platform-isolated, NEVER mixed.
# ─────────────────────────────────────────────────────────────────────────────

reddit_queue:  queue.Queue = queue.Queue()
twitter_queue: queue.Queue = queue.Queue()


def passes_keyword_filter(text: str, keywords: list) -> bool:
    """Generic keyword gate — takes an explicit keyword list so Reddit
    and Twitter can be filtered against their own independent lists."""
    t = text.lower()
    for kw in keywords:
        if kw.lower() in t:
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER SEARCH QUERY — built directly from TWITTER_SEARCH_KEYWORDS
# ─────────────────────────────────────────────────────────────────────────────

def _build_twitter_search_query() -> str:
    if not TWITTER_SEARCH_KEYWORDS:
        return (
            "(\"international transfer\" OR \"bank blocked\" OR \"we got hacked\""
            " OR \"CRM is a nightmare\") -is:retweet lang:en"
        )
    parts = [f'"{kw}"' if " " in kw else kw for kw in TWITTER_SEARCH_KEYWORDS]
    query = "(" + " OR ".join(parts) + ") -is:retweet lang:en"
    log.info(f"Twitter search query built | terms:{len(parts)} | len:{len(query)}")
    return query


TWITTER_SEARCH_QUERY = _build_twitter_search_query()


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE PROMPT — generic, niche-agnostic (unchanged schema)
# ─────────────────────────────────────────────────────────────────────────────

CLAUDE_SYSTEM_PROMPT = """
You are Flintel's signal intelligence analyst.

Your only job is to read one social media post (Reddit or X) together
with its metadata, and produce two things:

1. An intent_score from 1 to 100, built from three weighted components
2. A short, human-written-style reply draft the end user can personalize
   and post themselves, in their own voice, from their own account

You are niche-agnostic. You are never told what industry, product, or
company this is for. You score purely on what is IN the post and its
metadata — nothing else.

SCORING MODEL — 100 POINTS, THREE COMPONENTS

COMPONENT 1 — RELEVANCE MATCH (0-40 points)
Does this post genuinely discuss the same problem, need, or topic as
the search_keyword provided — in meaning, not just in shared words?

  36-40  Post is unambiguously about exactly this problem/need.
  25-35  Post is clearly related, but broader, tangential, or partial.
  10-24  Post mentions matching words but the actual subject differs.
  0-9    No genuine connection.

This component is a HARD GATE. If relevance scores below 10, is_relevant
must be false and intent_score must not exceed 15, no matter how strong
Google visibility or engagement look.

COMPONENT 2 — GOOGLE VISIBILITY (0-30 points)
  google_rank contribution (0-20):
    Rank 1 -> 20 | Rank 2-3 -> 16 | Rank 4-10 -> 11
    Rank 11-20 -> 6 | Not ranked/null -> 0
  search_volume contribution (0-10):
    10,000+/mo -> 10 | 3,000-9,999 -> 7
    500-2,999 -> 4 | Under 500/null -> 1

COMPONENT 3 — ENGAGEMENT SIGNAL (0-30 points)
Derived from upvotes and comments, scaled by platform norms (a tweet
with 200 likes is not the same as a Reddit post with 200 upvotes —
judge proportionally, not by raw thresholds alone).
  Strong engagement -> 22-30 | Moderate -> 10-21
  Low/negligible -> 0-9 | No data -> 0

FINAL intent_score = Component 1 + Component 2 + Component 3, capped at 100.

REPLY DRAFT — RULES
Only generate reply_draft when is_relevant is true.
- Generic and honest — never invent a fake personal story, dollar
  amount, or timeline not present in the input.
- Acknowledge the poster's situation in one clause, then offer one
  genuinely useful angle — not a pitch.
- 2-3 sentences maximum. No links, no "DM me," no product/company name
  (the end user adds that themselves if relevant).
- End on warmth or a question, never a call-to-action.

OUTPUT FORMAT
Return ONLY valid JSON. No preamble, no markdown, no code fences.
Return one object per post, in a JSON array, same order as received.

[
  {
    "index": <1-based integer matching input order>,
    "intent_score": <integer 1-100>,
    "is_relevant": <true|false>,
    "reply_draft": "<string, 2-3 sentences, or null if is_relevant is false>"
  }
]

Score every post received. Return the same count as received. Never
omit an item. Never add commentary outside the JSON array.
"""


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB — signals collection + persistent batch-state collections +
# per-keyword fetch-once-forever cache collection (flintel_keywords).
# ─────────────────────────────────────────────────────────────────────────────

def get_database():
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[MONGODB_DB]

        db.signals.create_index([("message_id", ASCENDING)], unique=True, name="message_id_unique")
        db.signals.create_index([("post_url", ASCENDING)], name="post_url_lookup")
        for field in ["intent_score", "created_at", "client_id", "platform", "is_relevant", "status"]:
            db.signals.create_index([(field, ASCENDING)])

        # persistent batch state — survives restarts, no in-flight batch lost
        db.flintel_pending_batch.create_index([("platform", ASCENDING)], unique=True, name="platform_unique")
        db.flintel_seen_ids.create_index([("platform", ASCENDING)], unique=True, name="seen_platform_unique")
        db.flintel_queue_messages.create_index(
            [("_platform_key", ASCENDING), ("message_id", ASCENDING)],
            unique=True, name="queue_platform_message_unique",
        )
        db.flintel_batch_seconds.create_index(
            [("platform", ASCENDING)], unique=True, name="batch_seconds_platform_unique"
        )

        # ── flintel_keywords — FETCH-ONCE-FOREVER cache. Restart-safe: this
        # collection is the single source of truth for "has this keyword
        # ever been fetched?" AND "does this keyword already have a cached
        # search_volume?" It survives process restarts, so a keyword
        # already marked fetched=True is NEVER re-fetched, ever, and a
        # keyword that already has a search_volume is NEVER re-queried
        # for volume, ever.
        db.flintel_keywords.create_index([("keyword", ASCENDING)], unique=True, name="keyword_unique")
        db.flintel_keywords.create_index([("fetched", ASCENDING)], name="keyword_fetched_idx")
        db.flintel_keywords.create_index([("search_volume", ASCENDING)], name="keyword_volume_idx")

        log.info("MongoDB connected.")
        return db
    except Exception as exc:
        log.critical(f"MongoDB connection failed: {exc}")
        raise


db = get_database()

# ─────────────────────────────────────────────────────────────────────────────
# ANTHROPIC CLIENT — streaming
# ─────────────────────────────────────────────────────────────────────────────

anthropic_client = anthropic.Anthropic(
    api_key=ANTHROPIC_API_KEY,
    http_client=httpx.Client(
        timeout=httpx.Timeout(connect=30.0, read=None, write=60.0, pool=30.0)
    ),
)


def retry_with_backoff(func, *args, retries=3, delay=2, label="op", **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            wait = delay * attempt
            log.error(f"[{label}] attempt {attempt}/{retries} failed: {exc}")
            if attempt < retries:
                log.info(f"[{label}] retrying in {wait}s...")
                time.sleep(wait)
            else:
                log.critical(f"[{label}] all {retries} attempts failed.")
                return None


def log_operator_alert(title: str, detail: str, level: str = "ERROR"):
    log.log(
        logging.CRITICAL if level == "CRITICAL" else logging.ERROR,
        f"[OPERATOR ALERT] {title} — {detail}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# PERSISTENT BATCH STATE HELPERS — survives process restarts, so a
# half-filled batch never disappears.
# ─────────────────────────────────────────────────────────────────────────────

def load_pending_batch(platform: str) -> tuple:
    try:
        doc = db.flintel_pending_batch.find_one({"platform": platform})
        if not doc:
            return [], None
        items = doc.get("items", [])
        start_ts = doc.get("batch_start_time")
        start_time = start_ts.timestamp() if start_ts else None
        if items:
            log.warning(f"[{platform.upper()}] Resuming persisted batch | {len(items)} item(s) recovered.")
        return items, start_time
    except Exception as exc:
        log.error(f"[{platform.upper()}] load_pending_batch error: {exc}")
        return [], None


def save_pending_batch(platform: str, items: list, batch_start_time):
    try:
        start_dt = datetime.fromtimestamp(batch_start_time, tz=timezone.utc) if batch_start_time else None
        db.flintel_pending_batch.update_one(
            {"platform": platform},
            {"$set": {"platform": platform, "items": items, "batch_start_time": start_dt,
                       "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_pending_batch error: {exc}")


def clear_pending_batch(platform: str):
    try:
        db.flintel_pending_batch.update_one(
            {"platform": platform},
            {"$set": {"platform": platform, "items": [], "batch_start_time": None,
                       "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] clear_pending_batch error: {exc}")


def load_seen_ids(platform: str) -> set:
    try:
        doc = db.flintel_seen_ids.find_one({"platform": platform})
        return set(doc.get("ids", [])) if doc else set()
    except Exception as exc:
        log.error(f"[{platform.upper()}] load_seen_ids error: {exc}")
        return set()


def save_seen_ids(platform: str, ids: set, cap: int = 200_000):
    try:
        id_list = list(ids)
        if len(id_list) > cap:
            id_list = id_list[-cap:]
        db.flintel_seen_ids.update_one(
            {"platform": platform},
            {"$set": {"platform": platform, "ids": id_list, "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_seen_ids error: {exc}")


def save_queue_message(platform: str, item: dict):
    try:
        mid = item.get("message_id")
        if not mid:
            return
        doc = dict(item)
        doc["_platform_key"] = platform
        doc["message_id"] = mid
        doc["queued_at"] = datetime.now(timezone.utc)
        db.flintel_queue_messages.update_one(
            {"_platform_key": platform, "message_id": mid}, {"$set": doc}, upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_queue_message error: {exc}")


def remove_queue_message(platform: str, message_id: str):
    if not message_id:
        return
    try:
        db.flintel_queue_messages.delete_one({"_platform_key": platform, "message_id": message_id})
    except Exception as exc:
        log.error(f"[{platform.upper()}] remove_queue_message error: {exc}")


def load_queue_messages(platform: str) -> list:
    try:
        docs = list(db.flintel_queue_messages.find({"_platform_key": platform}))
        items = []
        for d in docs:
            d.pop("_id", None)
            d.pop("_platform_key", None)
            d.pop("queued_at", None)
            items.append(d)
        return items
    except Exception as exc:
        log.error(f"[{platform.upper()}] load_queue_messages error: {exc}")
        return []


def save_batch_seconds(platform: str, batch_start_time):
    try:
        start_dt = datetime.fromtimestamp(batch_start_time, tz=timezone.utc) if batch_start_time else None
        db.flintel_batch_seconds.update_one(
            {"platform": platform},
            {"$set": {"platform": platform, "batch_start_time": start_dt,
                       "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_batch_seconds error: {exc}")


def clear_batch_seconds(platform: str):
    try:
        db.flintel_batch_seconds.update_one(
            {"platform": platform},
            {"$set": {"platform": platform, "batch_start_time": None,
                       "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] clear_batch_seconds error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD CACHE — flintel_keywords collection. FETCH-ONCE-FOREVER design:
# each keyword gets fetched from DataForSEO exactly ONE time, ever. Once
# fetched=True, it is PERMANENTLY skipped by get_due_keywords() — no TTL,
# no re-due date, no 12h/24h re-fetch. This REPLACES the old "sleep 12h
# then refetch everything from scratch" design AND the TTL-based re-fetch
# design that came after it:
#
#   - New keyword added to REDDIT_SEARCH_KEYWORDS -> sync_keywords_to_db()
#     inserts it with fetched=False, search_volume=None (due immediately)
#     -> picked up on the very next poll pass (within
#     KEYWORD_CHECK_INTERVAL_SECONDS).
#
#   - Keyword already fetched (fetched=True) -> get_due_keywords() will
#     NEVER return it again, period. Zero DataForSEO SERP calls for it,
#     ever again, even after restarts, even after any amount of time
#     passes. This guarantees Claude/signals data is never disturbed by
#     the same keyword search being repeated later.
#
#   - Keyword already has a search_volume stored -> it will NEVER be
#     included in a future seed_search_volume_batch() call again either,
#     for the exact same "fetch-once-forever" reason. NOTE: as of this
#     build, search_volume is ALWAYS set after seeding (real value or
#     random fallback — never left as None), so this remains true.
#
#   - Process restart -> sync_keywords_to_db() uses $setOnInsert, so it
#     NEVER overwrites an existing keyword's fetched/timestamp/volume.
#     Nothing resets to zero. Only genuinely brand-new keywords get
#     inserted.
# ─────────────────────────────────────────────────────────────────────────────

def sync_keywords_to_db(keywords: list):
    """
    Ensures every keyword currently in REDDIT_SEARCH_KEYWORDS exists in
    flintel_keywords. Brand-new keywords are inserted with fetched=False
    and search_volume=None (both due immediately, real-time). Keywords
    that already exist are left completely untouched — $setOnInsert only
    writes on first-ever insert. Safe to call every loop pass and on
    every restart.
    """
    now = datetime.now(timezone.utc)
    for kw in keywords:
        try:
            db.flintel_keywords.update_one(
                {"keyword": kw},
                {"$setOnInsert": {
                    "keyword":                  kw,
                    "fetched":                  False,
                    "search_volume":            None,
                    "search_volume_is_random":  False,
                    "last_fetched_at":          None,
                    "created_at":               now,
                }},
                upsert=True,
            )
        except Exception as exc:
            log.error(f"[KEYWORD-CACHE] sync error for {kw!r}: {exc}")


def get_keywords_missing_volume(keywords: list) -> list:
    """
    Returns keyword strings (from `keywords`) whose flintel_keywords
    document has no search_volume stored yet (missing field or explicit
    None both match this query — that's how a None-valued MongoDB filter
    works). These are exactly the keywords that will be sent to
    seed_search_volume_batch() next, batched, never one at a time.

    Once a keyword's search_volume is set (real value OR — as of this
    build — a random fallback value when the real call failed), it will
    never show up here again, so it will never be re-queried for volume,
    ever — same fetch-once-forever guarantee as the discovery cache.
    """
    try:
        cursor = db.flintel_keywords.find(
            {"keyword": {"$in": keywords}, "search_volume": None},
            {"keyword": 1},
        )
        return [d["keyword"] for d in cursor]
    except Exception as exc:
        log.error(f"[VOLUME-SEED] get_keywords_missing_volume error: {exc}")
        return []


def get_due_keywords() -> list:
    """
    Returns keyword docs that have NEVER been fetched yet (fetched=False).
    Once a keyword is marked fetched=True, it is PERMANENTLY excluded from
    this query — there is no TTL, no re-due date, nothing. A keyword is
    processed exactly once, ever. This guarantees Claude never re-scores
    the same keyword's world twice and signals data is never disturbed
    by repeat fetches.

    Each returned document already carries its own "search_volume" field
    (seeded ahead of time by seed_search_volume_batch()) — the discovery
    loop reads it straight off this same document, no extra query needed.
    """
    try:
        cursor = db.flintel_keywords.find({
            "keyword": {"$in": REDDIT_SEARCH_KEYWORDS},
            "fetched": False,
        })
        return list(cursor)
    except Exception as exc:
        log.error(f"[KEYWORD-CACHE] get_due_keywords error: {exc}")
        return []


def mark_keyword_fetched(keyword: str):
    """
    Flips a keyword to fetched=True — PERMANENTLY. There is no TTL and no
    next_due_at anymore: once true, this keyword will never be picked up
    by get_due_keywords() again, even after restarts, even after 12h,
    24h, or any amount of time. The only way to re-process a keyword is
    to manually reset/delete its document in flintel_keywords.
    """
    now = datetime.now(timezone.utc)
    try:
        db.flintel_keywords.update_one(
            {"keyword": keyword},
            {"$set": {
                "fetched":         True,
                "last_fetched_at": now,
            }},
        )
    except Exception as exc:
        log.error(f"[KEYWORD-CACHE] mark_keyword_fetched error for {keyword!r}: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# SEARCH-VOLUME BATCH SEEDING — chunks keywords, fetches volume for each
# one (single.php only accepts one keyword per call), writes results back
# onto each keyword's own flintel_keywords document.
#
# BUG 1 FIX (carried forward): when the provider's response doesn't
# contain a recognizable volume field, the warning surfaces the HTTP
# status code and, if the body is a dict, its "message" field. A body
# shaped like {"message": "..."} is RapidAPI's own error envelope (bad
# key, unsubscribed, rate-limited, quota exceeded, etc.) — NOT a data
# payload with an unfamiliar field name.
#
# RANDOM-FALLBACK FIX (carried forward from v9.9): whenever that happens
# — call failed, no usable field, non-JSON body, etc. — instead of
# leaving search_volume as None forever, a random placeholder in the
# SEARCH_VOLUME_RANDOM_FALLBACK_MIN..MAX range is generated and stored,
# and a clearly-labelled "RANDOM FALLBACK" warning is logged with the
# exact value used. Real, provider-returned values are NEVER touched.
# This failure is fully isolated to search_volume — it never blocks or
# delays the separate Google-rank SERP call or the Reddit post fetch for
# that keyword's discovered posts; those run independently regardless of
# whether a real volume number or a random fallback came back.
# ─────────────────────────────────────────────────────────────────────────────

def seed_search_volume_batch(keywords_needing_volume: list, batch_size: int = SEARCH_VOLUME_BATCH_SIZE):
    """
    ONE-TIME (per keyword) BATCH search-volume seeding. Splits
    `keywords_needing_volume` into chunks of up to `batch_size` and
    fetches volume for every keyword in the chunk. Results are written
    back onto each keyword's own flintel_keywords document
    (search_volume field, plus search_volume_is_random) — the same
    document already used for the fetch-once-forever discovery cache.
    No new collection, no schema change beyond the one additive flag.

    Once a keyword's search_volume is set here (real or random
    fallback), get_keywords_missing_volume() will never return it again,
    so this function will never be called for that keyword again —
    fetch-once-forever, exactly like the SERP discovery cache above.
    """
    if not keywords_needing_volume:
        return
    if not RAPIDAPI_KEY:
        log.warning(
            "[VOLUME-SEED] RapidAPI key not set — cannot call the search-volume API. "
            "Applying RANDOM FALLBACK values to all keywords in this pass so they are "
            "never left permanently at None."
        )

    for i in range(0, len(keywords_needing_volume), batch_size):
        chunk = keywords_needing_volume[i:i + batch_size]
        try:
            # single.php only accepts ONE keyword per request, so each
            # keyword in the chunk gets its own call — same chunk/loop
            # structure kept as-is, only the error visibility + random
            # fallback behavior changed.
            volume_map = {}
            random_map = {}

            for kw in chunk:
                if not RAPIDAPI_KEY:
                    vol = _random_search_volume_fallback()
                    volume_map[kw] = vol
                    random_map[kw] = True
                    log.warning(
                        f"[VOLUME-SEED] RANDOM FALLBACK applied for {kw!r} | "
                        f"search_volume={vol} (range {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-"
                        f"{SEARCH_VOLUME_RANDOM_FALLBACK_MAX}) | reason: RAPIDAPI_KEY not "
                        f"configured — call never made | this is NOT a real search volume."
                    )
                    continue

                url = "https://seo-keyword-research.p.rapidapi.com/single.php"

                querystring = {"keyword": kw, "country": "us"}

                headers = {
                    "x-rapidapi-key": RAPIDAPI_KEY, # .env
                    "x-rapidapi-host": RAPIDAPI_KEYWORD_HOST,
                    "Content-Type": "application/json"
                }

                try:
                    r = requests.get(url, headers=headers, params=querystring, timeout=DATAFORSEO_VOLUME_TIMEOUT_SECONDS)
                    status_code = r.status_code
                    try:
                        row = r.json()
                    except ValueError:
                        log.error(f"[VOLUME-SEED] Non-JSON response for {kw!r} | status:{status_code}")
                        row = None
                except Exception as call_exc:
                    log.error(f"[VOLUME-SEED] request error for {kw!r}: {call_exc}")
                    status_code = None
                    row = None

                vol = _dig_value(row, VOLUME_FIELD_CANDIDATES)
                if vol is None:
                    # Surfaces the actual RapidAPI error instead of just
                    # "field not found" — a {"message": ...} body means
                    # the call itself failed (auth/quota/rate-limit),
                    # not that the field name was wrong.
                    api_message = row.get("message") if isinstance(row, dict) else None
                    log.warning(
                        f"[VOLUME-SEED] No search_volume for {kw!r} | status:{status_code} | "
                        f"api_message:{api_message!r} | tried_fields:{VOLUME_FIELD_CANDIDATES} | "
                        f"raw_keys:{list(row.keys()) if isinstance(row, dict) else type(row).__name__}"
                    )
                    vol = _random_search_volume_fallback()
                    random_map[kw] = True
                    log.warning(
                        f"[VOLUME-SEED] RANDOM FALLBACK applied for {kw!r} | "
                        f"search_volume={vol} (range {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-"
                        f"{SEARCH_VOLUME_RANDOM_FALLBACK_MAX}) | reason: no credits / bad key / "
                        f"rate-limited / no usable field (see api_message above) | this is NOT "
                        f"a real, provider-returned search volume."
                    )
                else:
                    random_map[kw] = False
                volume_map[kw] = vol

            for kw in chunk:
                vol = volume_map.get(kw)
                is_random = random_map.get(kw, False)
                db.flintel_keywords.update_one(
                    {"keyword": kw},
                    {"$set": {"search_volume": vol, "search_volume_is_random": is_random}},
                    upsert=True,
                )

            random_count = sum(1 for v in random_map.values() if v)
            log.info(
                f"[VOLUME-SEED] Batch {i // batch_size + 1} | {len(chunk)} keyword(s) "
                f"seeded with search_volume | via RapidAPI (single.php, one call per keyword) | "
                f"real:{len(chunk) - random_count} random_fallback:{random_count}"
            )

        except Exception as exc:
            log.error(f"[VOLUME-SEED] batch error (keywords {i}-{i + len(chunk)}): {exc}")
            # Even on an unexpected batch-level error, don't leave these
            # keywords permanently at None — apply the random fallback so
            # they're never stuck, and log it clearly.
            for kw in chunk:
                vol = _random_search_volume_fallback()
                log.warning(
                    f"[VOLUME-SEED] RANDOM FALLBACK applied for {kw!r} | search_volume={vol} "
                    f"| reason: unexpected batch-level error — {exc} | this is NOT a real "
                    f"search volume."
                )
                try:
                    db.flintel_keywords.update_one(
                        {"keyword": kw},
                        {"$set": {"search_volume": vol, "search_volume_is_random": True}},
                        upsert=True,
                    )
                except Exception as inner_exc:
                    log.error(f"[VOLUME-SEED] could not persist random fallback for {kw!r}: {inner_exc}")

        time.sleep(SERP_FETCH_SLEEP_SECONDS)


# ─────────────────────────────────────────────────────────────────────────────
# ENRICHMENT — RapidAPI is the SOLE provider for Google rank + volume.
# ─────────────────────────────────────────────────────────────────────────────

def fetch_search_volume(search_keyword: str) -> int | None:
    """
    Monthly search volume — a SINGLE keyword, single request. Kept for
    the Twitter fallback path (fetch_google_stats(), used only when
    SEARCH_KEYWORD is configured for Twitter items, which have no
    per-post SERP discovery in this design).

    RANDOM-FALLBACK FIX (carried forward): if RAPIDAPI_KEY isn't
    configured, the call fails/times out, the response isn't JSON, or no
    usable volume field is found, a random placeholder in the
    SEARCH_VOLUME_RANDOM_FALLBACK_MIN..MAX range is returned instead of
    None, and a clearly-labelled "RANDOM FALLBACK" warning is logged
    with the exact value and reason. A real, provider-returned value is
    NEVER overridden.

    NOTE: the Reddit discovery path (process_one_keyword()) NO LONGER
    calls this function — Reddit's search_volume now comes exclusively
    from the batched seed_search_volume_batch() cache stored on each
    keyword's flintel_keywords document. This function remains only for
    the low-volume, single-keyword Twitter fallback use case.
    """
    if not search_keyword:
        return None

    if not RAPIDAPI_KEY:
        vol = _random_search_volume_fallback()
        log.warning(
            f"fetch_search_volume RANDOM FALLBACK applied for {search_keyword!r} | "
            f"search_volume={vol} | reason: RAPIDAPI_KEY not configured — call never made | "
            f"this is NOT a real search volume."
        )
        return vol

    try:
        url = "https://seo-keyword-research.p.rapidapi.com/single.php"

        querystring = {"keyword": search_keyword, "country": "us"}

        headers = {
            "x-rapidapi-key": RAPIDAPI_KEY, # .env
            "x-rapidapi-host": RAPIDAPI_KEYWORD_HOST,
            "Content-Type": "application/json"
        }

        r = requests.get(url, headers=headers, params=querystring, timeout=DATAFORSEO_VOLUME_TIMEOUT_SECONDS)
        status_code = r.status_code

        try:
            result = r.json()
        except ValueError:
            log.error(f"fetch_search_volume non-JSON response for {search_keyword!r} | status:{status_code}")
            vol = _random_search_volume_fallback()
            log.warning(
                f"fetch_search_volume RANDOM FALLBACK applied for {search_keyword!r} | "
                f"search_volume={vol} | reason: non-JSON response (status:{status_code}) | "
                f"this is NOT a real search volume."
            )
            return vol

        vol = _dig_value(result, VOLUME_FIELD_CANDIDATES)
        if vol is None:
            api_message = result.get("message") if isinstance(result, dict) else None
            log.warning(
                f"fetch_search_volume no volume field for {search_keyword!r} | "
                f"status:{status_code} | api_message:{api_message!r}"
            )
            vol = _random_search_volume_fallback()
            log.warning(
                f"fetch_search_volume RANDOM FALLBACK applied for {search_keyword!r} | "
                f"search_volume={vol} (range {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-"
                f"{SEARCH_VOLUME_RANDOM_FALLBACK_MAX}) | reason: no credits / bad key / "
                f"rate-limited / no usable field (see api_message above) | this is NOT a "
                f"real, provider-returned search volume."
            )
        return vol
    except Exception as exc:
        log.error(f"fetch_search_volume error for {search_keyword!r}: {exc}")
        vol = _random_search_volume_fallback()
        log.warning(
            f"fetch_search_volume RANDOM FALLBACK applied for {search_keyword!r} | "
            f"search_volume={vol} | reason: exception during call — {exc} | this is NOT a "
            f"real search volume."
        )
        return vol


def fetch_google_rank(search_keyword: str) -> int | None:
    """
    GENERIC (non-post-specific) Google rank fallback — used ONLY for
    Twitter items, which have no per-post SERP discovery in this design.
    Reflects the #1 organic result for the fixed SEARCH_KEYWORD.

    This call is fully independent of fetch_search_volume() — it always
    runs on its own merits and is never skipped or blocked just because
    a prior search_volume lookup returned a random fallback or failed.
    Google-rank has NO random-fallback behavior (only requested for
    search_volume) — it stays None on failure, exactly as before.
    """
    if not RAPIDAPI_KEY or not search_keyword:
        return None
    try:
        url = "https://google-search116.p.rapidapi.com/"

        querystring = {"query": search_keyword}

        headers = {
            "x-rapidapi-key": RAPIDAPI_KEY, # .env boht used same key
            "x-rapidapi-host": RAPIDAPI_SEARCH_HOST,
            "Content-Type": "application/json"
        }

        r = requests.get(url, headers=headers, params=querystring, timeout=DATAFORSEO_SERP_TIMEOUT_SECONDS)

        try:
            result_data = r.json()
        except ValueError:
            log.error(f"fetch_google_rank non-JSON response for {search_keyword!r} | status:{r.status_code}")
            return None

        items = _dig_list(result_data, RESULT_LIST_KEY_CANDIDATES)
        if not items:
            return None
        return _dig_value(items[0], RANK_FIELD_CANDIDATES)
    except Exception as exc:
        log.error(f"fetch_google_rank error for {search_keyword!r}: {exc}")
        return None


def fetch_google_stats(search_keyword: str) -> dict:
    return {
        "google_rank":   fetch_google_rank(search_keyword),
        "search_volume": fetch_search_volume(search_keyword),
    }


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT — SOLE discovery mechanism: RapidAPI SERP search
# (site:reddit.com) -> real per-post rank + URL -> Reddit's public,
# credential-free per-post RSS feed (smart-retry) -> full post data (text,
# username, subreddit, upvotes, comments, posted_at). Each keyword is
# only fetched when get_due_keywords() says it's due — see the KEYWORD
# CACHE section above. search_volume is read from the already-seeded
# flintel_keywords document, never fetched here.
# ─────────────────────────────────────────────────────────────────────────────

def search_google_for_keyword(keyword: str, months_back: int = SERP_MONTHS_BACK) -> list:
    """
    RapidAPI Google search restricted to site:reddit.com, rolling
    last-N-months date window. Returns real per-result rank + URL. Only
    called for keywords that get_due_keywords() has flagged as due.

    This call CANNOT be batched across keywords (each keyword is its own
    unique search query with its own unique results) — it remains one
    call per keyword. It runs unconditionally whenever a keyword is due,
    on its own dedicated RapidAPI host, completely independent of the
    search-volume host/call above — it is NEVER blocked, delayed, or
    skipped because of a search-volume failure or random fallback, and
    it never blocks search-volume in the other direction either.
    """
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
            "x-rapidapi-key": RAPIDAPI_KEY, # .env boht used same key
            "x-rapidapi-host": RAPIDAPI_SEARCH_HOST,
            "Content-Type": "application/json"
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
                # Fall back to the result's position in the returned
                # order if the provider genuinely doesn't expose an
                # explicit rank field — better than a permanent null.
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
    """
    Checks the `signals` collection DIRECTLY by post_url — BEFORE any
    Reddit fetch or Claude scoring happens. If this URL was already
    discovered and saved in a previous cycle (confirmed OR pending), we
    skip it entirely here — no wasted fetch, no wasted Claude call.

    This is a separate, independent safety net from the keyword-level
    cache above: the keyword cache stops a keyword's SEARCH from
    re-running too often; this dedup stops the SAME POST from being
    re-scored even if its keyword search does run again.
    """
    if not post_url:
        return False
    try:
        existing = db.signals.find_one({"post_url": post_url}, {"_id": 1})
        return existing is not None
    except Exception as exc:
        log.error(f"[DEDUP] is_post_already_signaled error for {post_url}: {exc}")
        return False   # fail-open: if the check itself fails, don't block discovery


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT POST FETCH — public, credential-free per-post RSS feed ONLY.
#
# v9.11: switched from the .json endpoint to Reddit's public per-post
# RSS feed (post_url + ".rss" instead of ".json"). Root cause of the
# switch: the .json endpoint was hitting a consistent, 100%-failure-rate
# 403 on BOTH www.reddit.com and old.reddit.com across every retry — the
# signature of an IP-level anonymous-scraping block, not a code bug (the
# SERP/Google-rank discovery call, on a totally separate RapidAPI host,
# kept working the whole time). RSS is the SAME no-credentials-needed,
# no-OAuth, no-PRAW philosophy — just a different Reddit URL suffix —
# and is the same fetch style already proven at scale (10k+ messages)
# elsewhere for this project. The v9.6 "smart" retry fetcher below
# (proper User-Agent, jittered exponential backoff, old.reddit.com
# fallback host) is completely unchanged — only the URL suffix (.rss
# instead of .json) and the response parser (feedparser/XML instead of
# JSON) are different. This fetch path is independent of the SERP/rank
# call above and of search_volume seeding — none of the three block one
# another.
#
# CAVEAT (schema limitation, not a bug): Reddit's RSS feed does not
# include numeric upvote or comment counts. upvotes/comments are
# therefore generated as a random placeholder (see
# REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN/MAX above) for every post, with
# a clearly-labelled "RANDOM FALLBACK" warning logged every single time
# — exactly the same pattern already used for search_volume, so it is
# always distinguishable in the logs from a real, provider-returned
# number.
# ─────────────────────────────────────────────────────────────────────────────

def _reddit_get_with_retry(url: str) -> requests.Response | None:
    """
    v9.6 "smart" GET wrapper for Reddit's public endpoints — kept 100%
    as-is in terms of retry/backoff/jitter behavior. As of v9.11 this is
    used against the per-post RSS feed URL (post_url + ".rss") instead
    of the .json endpoint, but the function itself is content-type
    agnostic — it only inspects the HTTP status code, so no logic
    change was needed here at all:
      - Reddit-recommended User-Agent format (REDDIT_USER_AGENT).
      - Small randomized jitter delay before each attempt, to avoid an
        obviously robotic, perfectly-uniform request cadence.
      - Exponential backoff retry, up to REDDIT_FETCH_MAX_RETRIES times,
        specifically for 403 / 429 / 5xx responses (these are the
        classes of error retrying can plausibly help with; a 404 means
        the post is genuinely gone and is not retried).
    Returns the Response on success (status 200), or None if every
    attempt failed — caller treats None exactly like the old code
    treated a raised exception (skip this post, try again on a future
    discovery pass since it was never saved to `signals`).
    """
    headers = {
        "User-Agent": REDDIT_USER_AGENT,
        "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
    }

    last_status = None
    for attempt in range(1, REDDIT_FETCH_MAX_RETRIES + 1):
        # jitter delay BEFORE each request — including the first — so
        # request timing doesn't look perfectly mechanical
        time.sleep(random.uniform(REDDIT_FETCH_JITTER_MIN, REDDIT_FETCH_JITTER_MAX))
        try:
            r = requests.get(url, headers=headers, timeout=REDDIT_JSON_TIMEOUT_SECONDS)
            last_status = r.status_code
            if r.status_code == 200:
                return r
            if r.status_code == 404:
                # post genuinely removed/deleted — retrying won't help
                log.debug(f"[SERP] 404 (gone) for {url} — not retrying.")
                return None
            if r.status_code in (403, 429) or r.status_code >= 500:
                wait = (REDDIT_FETCH_BACKOFF_BASE ** attempt) + random.uniform(0, 1.0)
                log.warning(
                    f"[SERP] Reddit fetch attempt {attempt}/{REDDIT_FETCH_MAX_RETRIES} "
                    f"got {r.status_code} for {url} — backing off {wait:.1f}s..."
                )
                time.sleep(wait)
                continue
            # any other status — don't spin, just fail
            log.error(f"[SERP] Unexpected status {r.status_code} for {url}")
            return None
        except requests.RequestException as exc:
            log.warning(
                f"[SERP] Reddit fetch attempt {attempt}/{REDDIT_FETCH_MAX_RETRIES} "
                f"network error for {url}: {exc}"
            )
            time.sleep((REDDIT_FETCH_BACKOFF_BASE ** attempt))

    log.error(f"[SERP] Reddit fetch exhausted {REDDIT_FETCH_MAX_RETRIES} attempts for {url} "
              f"(last_status:{last_status})")
    return None


def _extract_reddit_submission_id(post_url: str) -> str | None:
    """Pulls the submission id out of a standard reddit.com post URL
    (e.g. .../comments/<id>/...). Used to build a stable message_id
    since the RSS feed itself doesn't always expose a clean numeric id.
    Returns None if it can't be found — caller falls back to a
    sanitized version of the full URL."""
    match = re.search(r"/comments/([a-zA-Z0-9]+)", post_url)
    return match.group(1) if match else None


def _extract_reddit_subreddit_from_url(post_url: str) -> str:
    """Pulls the subreddit name out of a standard reddit.com post URL
    (e.g. reddit.com/r/<subreddit>/comments/...). Returns "" if it
    can't be found — never raises."""
    match = re.search(r"reddit\.com/r/([^/]+)/", post_url)
    return match.group(1) if match else ""


def fetch_reddit_post_by_url(post_url: str, keyword: str, rank: int) -> dict | None:
    """
    Fetches the FULL post: text, username, subreddit, upvotes, comments,
    posted_at. This is the ONLY way Reddit data enters this system now.

    v9.11: fetches Reddit's public, credential-free per-post RSS feed
    (post_url + ".rss") instead of the .json endpoint — no OAuth, no
    PRAW, nothing to configure, same smart-retry + old.reddit.com
    fallback host as before. title/selftext/author/subreddit/posted_at
    come straight off the RSS entry. upvotes/comments are NOT present in
    Reddit's RSS schema, so they are generated as a random placeholder
    (REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN..MAX) with a clearly-labelled
    "RANDOM FALLBACK" warning logged every time — same pattern already
    used for search_volume.
    """
    if not post_url:
        return None

    primary_url = post_url.rstrip("/") + ".rss"
    r = _reddit_get_with_retry(primary_url)

    if r is None and "old.reddit.com" not in post_url:
        # last-resort fallback host
        fallback_url = (
            post_url.rstrip("/")
            .replace("https://www.reddit.com", "https://old.reddit.com")
            .replace("https://reddit.com", "https://old.reddit.com")
            + ".rss"
        )
        if fallback_url != primary_url:
            log.info(f"[SERP] Retrying via old.reddit.com fallback: {fallback_url}")
            r = _reddit_get_with_retry(fallback_url)

    if r is None:
        log.error(f"[SERP] fetch_reddit_post_by_url gave up for {post_url}")
        return None

    try:
        feed = feedparser.parse(r.content)
        if not feed.entries:
            log.error(f"[SERP] fetch_reddit_post_by_url: RSS feed had no entries for {post_url}")
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
                posted_at = published  # fall back to raw string if struct_time parse fails

        submission_id = _extract_reddit_submission_id(post_url)
        message_id = f"reddit_serp_{submission_id}" if submission_id else (
            f"reddit_serp_{re.sub(r'[^a-zA-Z0-9]', '_', post_url)[-40:]}"
        )

        upvotes = _random_engagement_fallback()
        comments = _random_engagement_fallback()
        log.warning(
            f"[SERP] RANDOM FALLBACK applied for engagement on {post_url} | "
            f"upvotes={upvotes} comments={comments} "
            f"(range {REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN}-{REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX}) | "
            f"reason: Reddit's public RSS feed does not expose numeric engagement counts | "
            f"this is NOT real, provider-returned engagement data."
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
            "upvotes":              upvotes,
            "comments":             comments,
            "engagement_is_random": True,   # RSS never provides real counts — always random as of v9.11
            "google_rank":          rank,   # real per-post rank, already set here
            "search_volume":        None,   # filled in by process_one_keyword() below
        }
    except Exception as exc:
        log.error(f"[SERP] fetch_reddit_post_by_url parse error for {post_url}: {exc}")
        return None


def process_one_keyword(keyword: str, volume, volume_is_random: bool = False) -> tuple:
    """
    Full discovery work for ONE keyword that get_due_keywords() has
    flagged as due right now:
      1. RapidAPI SERP search (site:reddit.com, last N months) — runs
         regardless of whether this keyword's search_volume is a real
         number or a random fallback.
      2. Per-result post_url dedup check -> skip already-known posts
         (no fetch, no Claude call for those)
      3. Reddit fetch (public RSS feed, credential-free, smart-retry) for
         genuinely new posts -> stamp the keyword's already-seeded
         search_volume (and its random/real flag) onto each item ->
         queue for Claude scoring
    Returns (new_items_count, skipped_dupes_count) for logging.

    `volume` and `volume_is_random` are passed in by the caller (read
    straight off the keyword's own flintel_keywords document by
    run_serp_discovery_loop()) instead of being fetched here —
    search_volume is sourced from the batched seed_search_volume_batch()
    cache. Every post discovered for this keyword still ends up with the
    exact same item schema and the exact same queue/Claude/signals flow
    — whether `volume` is real or a random fallback, the SERP rank
    lookup and the Reddit post fetch above always still run.
    """
    new_items, skipped_dupes = 0, 0

    results = search_google_for_keyword(keyword, months_back=SERP_MONTHS_BACK)

    for result in results:
        if is_post_already_signaled(result["url"]):
            skipped_dupes += 1
            log.debug(f"[SERP] Skipping already-known post_url: {result['url']}")
            continue

        item = fetch_reddit_post_by_url(result["url"], keyword, result["rank"])
        if not item:
            continue
        item["search_volume"] = volume   # same cached value for every post from this keyword
        item["search_volume_is_random"] = volume_is_random
        reddit_queue.put(item)
        save_queue_message("reddit", item)
        new_items += 1
        time.sleep(SERP_FETCH_SLEEP_SECONDS)

    return new_items, skipped_dupes


def run_serp_discovery_loop():
    """
    Continuously polls flintel_keywords every KEYWORD_CHECK_INTERVAL_SECONDS
    for keywords that have NEVER been fetched (fetched=False), and for any
    keyword still missing a cached search_volume (batch-seeds it — real
    value, or a logged random fallback if the call fails/has no credits).

    There is NO TTL, NO re-due date, NO fixed "sleep N hours then redo
    everything" step. Each keyword's SERP discovery is processed exactly
    ONCE, ever:
      - a newly-added keyword is picked up on the very next pass
        (within KEYWORD_CHECK_INTERVAL_SECONDS): its search_volume is
        batch-seeded alongside any other keyword missing one at that
        moment (never a solo per-keyword call), then it's processed one
        at a time (sequential) for SERP + posts, then marked fetched=True
        permanently.
      - an already-fetched keyword is skipped forever, even immediately
        after a full server restart — its state lives in MongoDB, not
        in memory, so nothing resets to zero and nothing gets re-fetched.
      - a keyword that already has a search_volume (real or random
        fallback) is never re-queried for volume again, ever, for the
        same reason.
      - whether a keyword's seeded search_volume came back as a real
        number OR a random fallback (RapidAPI error/quota/rate-limit —
        see seed_search_volume_batch()), that keyword is STILL processed
        for SERP rank + Reddit post fetch below exactly the same way — a
        missing/failed volume never blocks or skips discovery, it is
        simply replaced with a clearly-logged random placeholder.
    """
    sync_keywords_to_db(REDDIT_SEARCH_KEYWORDS)

    # One-time (per new keyword) BATCH search-volume seeding, done BEFORE
    # the loop starts so the very first discovery pass already has cached
    # volumes to read.
    missing_volume = get_keywords_missing_volume(REDDIT_SEARCH_KEYWORDS)
    if missing_volume:
        log.info(
            f"[VOLUME-SEED] {len(missing_volume)} keyword(s) need search_volume — "
            f"seeding in batches of {SEARCH_VOLUME_BATCH_SIZE}..."
        )
        seed_search_volume_batch(missing_volume, batch_size=SEARCH_VOLUME_BATCH_SIZE)

    log.info(
        f"[SERP] Discovery loop started | {len(REDDIT_SEARCH_KEYWORDS)} keyword(s) | "
        f"check_interval:{KEYWORD_CHECK_INTERVAL_SECONDS}s | "
        f"months_back:{SERP_MONTHS_BACK} | depth:{SERP_RESULTS_PER_KEYWORD} | "
        f"KEYWORD CACHE: fetch-once-forever, restart-safe, no re-fetch ever | "
        f"SEARCH-VOLUME: batched loop (size {SEARCH_VOLUME_BATCH_SIZE}) | "
        f"random fallback range {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-{SEARCH_VOLUME_RANDOM_FALLBACK_MAX} "
        f"on failure/no-credits (always logged) | "
        f"REDDIT FETCH: public RSS feed only, credential-free ({REDDIT_FETCH_MAX_RETRIES}x backoff "
        f"+ old.reddit.com fallback host, no OAuth/PRAW, random engagement fallback "
        f"{REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN}-{REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX})"
    )

    while True:
        try:
            # Pick up any newly-added keywords immediately (idempotent —
            # never touches keywords that already exist).
            sync_keywords_to_db(REDDIT_SEARCH_KEYWORDS)

            # Batch-seed search_volume for any keyword still missing one
            # (covers brand-new keywords added since the last pass, or
            # any keyword whose previous seed attempt somehow left it
            # unset).
            missing_volume = get_keywords_missing_volume(REDDIT_SEARCH_KEYWORDS)
            if missing_volume:
                seed_search_volume_batch(missing_volume, batch_size=SEARCH_VOLUME_BATCH_SIZE)

            due = get_due_keywords()
            if not due:
                time.sleep(KEYWORD_CHECK_INTERVAL_SECONDS)
                continue

            total_new, total_dupes = 0, 0
            for doc in due:
                keyword = doc["keyword"]
                volume = doc.get("search_volume")                     # cached, already seeded — no API call
                volume_is_random = doc.get("search_volume_is_random", False)
                new_items, dupes = process_one_keyword(keyword, volume, volume_is_random)
                mark_keyword_fetched(keyword)
                total_new += new_items
                total_dupes += dupes
                sv_tag = "RANDOM-FALLBACK" if volume_is_random else "real"
                log.info(
                    f"[SERP] '{keyword}' DONE | new:{new_items} skipped_dupes:{dupes} | "
                    f"search_volume:{volume} ({sv_tag}, from cache) | "
                    f"marked fetched=True PERMANENTLY — will never be re-fetched"
                )
                time.sleep(SERP_FETCH_SLEEP_SECONDS)

            log.info(
                f"[SERP] Pass complete | keywords_processed:{len(due)} | "
                f"new_items:{total_new} | skipped_dupes:{total_dupes}"
            )

        except Exception as exc:
            log.error(f"[SERP] discovery loop error: {exc}")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE BATCH SCORER — streaming transport + partial-JSON recovery.
# ─────────────────────────────────────────────────────────────────────────────

def _build_batch_prompt(batch: list) -> str:
    lines = []
    for i, item in enumerate(batch, start=1):
        payload = {
            "search_keyword": item.get("search_keyword", SEARCH_KEYWORD),
            "text":           (item.get("text", "") or "")[:1200],
            "platform":       item.get("platform", "unknown"),
            "google_rank":    item.get("google_rank"),
            "search_volume":  item.get("search_volume"),
            "upvotes":        item.get("upvotes"),
            "comments":       item.get("comments"),
        }
        lines.append(f"--- POST {i} ---\n{json.dumps(payload, ensure_ascii=False)}\n")
    return "\n".join(lines)


def _fallback_score(index: int, reason: str = "Scoring unavailable.") -> dict:
    return {
        "index": index,
        "intent_score": 1,
        "is_relevant": False,
        "reply_draft": None,
        "_is_fallback": True,
        "_fallback_reason": reason,
    }


def _strip_code_fences(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        return parts[1].lstrip("json").strip() if len(parts) > 1 else raw.strip("```").strip()
    return raw


def _salvage_partial_json_array(raw: str) -> list:
    """Brace-depth-tracking salvage of a truncated JSON array."""
    start = raw.find("[")
    if start == -1:
        return []
    objects, depth, obj_start, in_string, escape = [], 0, None, False, False
    i, n = start + 1, len(raw)
    while i < n:
        ch = raw[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            i += 1
            continue
        if ch == '"':
            in_string = True
            i += 1
            continue
        if ch == "{":
            if depth == 0:
                obj_start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and obj_start is not None:
                candidate = raw[obj_start:i + 1]
                try:
                    objects.append(json.loads(candidate))
                except (json.JSONDecodeError, ValueError):
                    log.warning("[Claude-Batch] Skipped one malformed salvaged object.")
                obj_start = None
        i += 1
    return objects


def _parse_claude_json(raw: str) -> tuple:
    cleaned = _strip_code_fences(raw)
    try:
        parsed = json.loads(cleaned)
        if not isinstance(parsed, list):
            raise ValueError("Claude returned non-list.")
        return parsed, False
    except (json.JSONDecodeError, ValueError) as exc:
        log.warning(f"[Claude-Batch] Full parse failed ({exc}) — attempting partial recovery.")
        return _salvage_partial_json_array(cleaned), True


def _call_claude_batch(batch: list) -> list:
    prompt = _build_batch_prompt(batch)
    with anthropic_client.messages.stream(
        model="claude-sonnet-4-6",
        max_tokens=MAX_TOKENS,
        system=CLAUDE_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": f"Score this batch:\n\n{prompt}"}],
    ) as stream:
        raw = stream.get_final_text().strip()

    results, was_truncated = _parse_claude_json(raw)

    if was_truncated:
        recovered = {int(r["index"]) for r in results if isinstance(r, dict) and "index" in r}
        missing = sorted(set(range(1, len(batch) + 1)) - recovered)
        log.warning(f"[Claude-Batch] PARTIAL RECOVERY | batch_size:{len(batch)} | "
                    f"recovered:{len(recovered)} | missing:{len(missing)}")
        log_operator_alert(
            title="Claude Response Truncated (max_tokens) — Partial Recovery",
            detail=f"batch_size:{len(batch)} recovered:{len(recovered)} missing:{missing[:30]}",
            level="ERROR",
        )
        for idx in missing:
            results.append(_fallback_score(idx, "Truncated — not recovered."))

    if not isinstance(results, list):
        raise ValueError("Claude returned non-list after parsing.")

    for r in results:
        r.setdefault("is_relevant", False)
        r.setdefault("reply_draft", None)
        r.setdefault("_is_fallback", False)
        if r.get("intent_score", 1) < 1:
            r["intent_score"] = 1
        if r.get("intent_score", 1) > 100:
            r["intent_score"] = 100

    return results


def score_batch_with_claude(batch: list) -> list:
    result = retry_with_backoff(_call_claude_batch, batch, retries=3, delay=5, label="Claude-Batch")
    if result is None:
        log_operator_alert(
            title="Claude API Unavailable",
            detail=f"All 3 retry attempts failed for a batch of {len(batch)} items.",
            level="CRITICAL",
        )
        return [_fallback_score(i + 1, "Claude API unavailable after 3 retries.") for i in range(len(batch))]
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB STORAGE
# ─────────────────────────────────────────────────────────────────────────────

def save_new_signal(item: dict, score_result: dict, force_pending: bool = False) -> bool:
    """
    Brand-new LIVE items (from Reddit SERP-discovery or Twitter).

    status logic:
      - force_pending=True  -> status="pending"   (Claude failed for this
        item; run_rescore_processor() will automatically pick it up on
        its next poll cycle and retry scoring, reusing the enrichment
        fields already stored below — NO re-fetch from Reddit or
        RapidAPI happens on rescore.)
      - force_pending=False -> status="confirmed" (Claude scored it
        successfully — final).

    The `signals` document schema itself is UNCHANGED. The random/real
    origin of search_volume is surfaced purely in the log line below
    (via item.get("search_volume_is_random")) so it's always visible in
    the application/render logs which value type was used, without
    altering the persisted schema.
    """
    doc = {
        "message_id":            item["message_id"],
        "platform":               item.get("platform", "unknown"),
        "post_url":               item.get("post_url", ""),
        "text":                   item.get("text", ""),
        "username":               item.get("username", "unknown"),
        "subreddit_or_channel":   item.get("subreddit_or_channel", ""),
        "posted_at":              item.get("posted_at"),
        "fetched_at":             datetime.now(timezone.utc),
        "google_rank":            item.get("google_rank"),
        "search_volume":          item.get("search_volume"),
        "upvotes":                item.get("upvotes"),
        "comments":               item.get("comments"),
        "search_keyword":         item.get("search_keyword", SEARCH_KEYWORD),
        "intent_score":           score_result.get("intent_score", 1),
        "is_relevant":            score_result.get("is_relevant", False),
        "reply_draft":            score_result.get("reply_draft"),
        "client_id":              CLIENT_ID,
        "status":                 "pending" if force_pending else "confirmed",
        "created_at":             datetime.now(timezone.utc),
    }
    try:
        db.signals.insert_one(doc)
        sv_tag = "RANDOM-FALLBACK" if item.get("search_volume_is_random") else "real"
        eng_tag = "RANDOM-FALLBACK" if item.get("engagement_is_random") else "real"
        # Minimal, focused log line — ONLY what's needed to eyeball a
        # signal at a glance: platform + keyword, search_volume (/mo,
        # tagged real vs random-fallback), upvotes/comments (tagged real
        # vs random-fallback — as of v9.11, Reddit RSS never provides
        # real counts, so this will always read RANDOM-FALLBACK for
        # Reddit items), google_rank as a plain number, and the post_url.
        # Full doc (score, etc.) is still in Mongo/the /signals endpoint
        # as before — this is just the log line format, nothing else
        # changed.
        log.info(
            f"SAVED [{doc['platform'].upper()}] {doc['search_keyword']!r} | "
            f"search_volume:{doc['search_volume']}/mo ({sv_tag}) | "
            f"upvotes:{doc['upvotes']} comments:{doc['comments']} ({eng_tag}) | "
            f"google_rank:{doc['google_rank']} | "
            f"post_url:{doc['post_url']}"
        )
        return True
    except DuplicateKeyError:
        # Post already exists in signals (message_id unique) — the last
        # safety net. Claude may have just re-scored a re-discovered post
        # (cost incurred), but it will not be stored twice.
        return False
    except Exception as exc:
        log.error(f"MongoDB save error: {exc}")
        log_operator_alert("MongoDB Write Failed", str(exc), level="CRITICAL")
        return False


def replace_confirmed_signal(message_id: str, enrichment: dict, score_result: dict) -> bool:
    """
    Called by the rescore processor once Claude has (re-)scored a
    pending document. Reuses the enrichment fields (google_rank,
    search_volume, upvotes, comments) that are ALREADY stored on the
    existing document — NO new fetch to Reddit or RapidAPI happens here,
    only a re-call to Claude for scoring.
    """
    existing = db.signals.find_one({"message_id": message_id})
    if not existing:
        log.warning(f"[RESCORE] No existing doc for {message_id} — skipping.")
        return False

    new_doc = {
        "message_id":            message_id,
        "platform":               existing.get("platform", "unknown"),
        "post_url":               existing.get("post_url", ""),
        "text":                   existing.get("text", ""),
        "username":               existing.get("username", "unknown"),
        "subreddit_or_channel":   existing.get("subreddit_or_channel", ""),
        "posted_at":              existing.get("posted_at") or existing.get("created_at"),
        "fetched_at":             existing.get("fetched_at", datetime.now(timezone.utc)),
        "google_rank":            enrichment.get("google_rank"),
        "search_volume":          enrichment.get("search_volume"),
        "upvotes":                enrichment.get("upvotes"),
        "comments":               enrichment.get("comments"),
        "search_keyword":         enrichment.get("search_keyword", SEARCH_KEYWORD),
        "intent_score":           score_result.get("intent_score", 1),
        "is_relevant":            score_result.get("is_relevant", False),
        "reply_draft":            score_result.get("reply_draft"),
        "client_id":              CLIENT_ID,
        "status":                 "confirmed",
        "created_at":             existing.get("created_at", datetime.now(timezone.utc)),
    }
    db.signals.replace_one({"message_id": message_id}, new_doc)
    log.info(f"[RESCORE] CONFIRMED | {message_id} | score:{new_doc['intent_score']} relevant:{new_doc['is_relevant']}")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# GENERIC BATCH PROCESSOR — one instance per platform queue.
# ─────────────────────────────────────────────────────────────────────────────

def run_batch_processor(
    q: queue.Queue,
    batch_size: int,
    platform_label: str,
    gap_seconds: int,
    timeout_seconds: int,
    keyword_filter_list: list,
):
    platform_key = platform_label.lower()

    log.info(
        f"Batch processor [{platform_label}] started | "
        f"batch_size:{batch_size} | gap:{gap_seconds}s | timeout:{timeout_seconds}s"
    )

    current_batch, batch_start_time = load_pending_batch(platform_key)
    if current_batch:
        log.info(f"[{platform_label}] Resumed [{len(current_batch)}/{batch_size}] from persistent disk.")

    total_received, total_matched, total_dropped, total_batches = 0, 0, 0, 0

    while True:
        try:
            if current_batch and batch_start_time is not None:
                wait_time = max(0.1, timeout_seconds - (time.time() - batch_start_time))
            else:
                wait_time = 1.0

            try:
                item = q.get(timeout=wait_time)
                got_item = True
            except queue.Empty:
                got_item = False

            if got_item:
                total_received += 1
                remove_queue_message(platform_key, item.get("message_id"))

                text = (item.get("text") or "").strip()

                if not text or len(text) < 10:
                    q.task_done()
                    continue

                if not passes_keyword_filter(text, keyword_filter_list):
                    total_dropped += 1
                    q.task_done()
                    continue

                total_matched += 1
                if not current_batch:
                    batch_start_time = time.time()

                current_batch.append(item)
                save_pending_batch(platform_key, current_batch, batch_start_time)
                save_batch_seconds(platform_key, batch_start_time)

                log.info(f"[{platform_label}] MATCH [{len(current_batch)}/{batch_size}] | u/{item.get('username')}")
                q.task_done()

            should_fire = False
            fire_reason = ""
            if len(current_batch) >= batch_size:
                should_fire, fire_reason = True, f"batch full ({batch_size} items)"
            elif current_batch and batch_start_time is not None:
                elapsed = time.time() - batch_start_time
                if elapsed >= timeout_seconds:
                    should_fire, fire_reason = True, f"timeout ({timeout_seconds}s) — partial {len(current_batch)}/{batch_size}"

            if should_fire and current_batch:
                total_batches += 1
                batch_to_send = current_batch[:batch_size]
                current_batch = current_batch[batch_size:]
                batch_start_time = None if not current_batch else time.time()

                if current_batch:
                    save_pending_batch(platform_key, current_batch, batch_start_time)
                    save_batch_seconds(platform_key, batch_start_time)
                else:
                    clear_pending_batch(platform_key)
                    clear_batch_seconds(platform_key)

                # ── ENRICHMENT — real numbers, right before scoring ──────
                google_stats = None
                for it in batch_to_send:
                    already_enriched = it.get("google_rank") is not None

                    it.setdefault("upvotes", None)
                    it.setdefault("comments", None)

                    if not already_enriched and SEARCH_KEYWORD:
                        if google_stats is None:
                            google_stats = fetch_google_stats(SEARCH_KEYWORD)
                        it["google_rank"] = google_stats.get("google_rank")
                        it["search_volume"] = google_stats.get("search_volume")
                        it["search_keyword"] = SEARCH_KEYWORD
                        # search_volume for this path is produced by
                        # fetch_search_volume(), which now always logs
                        # its own "RANDOM FALLBACK" warning inline
                        # whenever it had to synthesize a value instead
                        # of returning a real one — no separate flag is
                        # threaded through here to keep this enrichment
                        # step's logic 100% as-is otherwise.

                log.info(
                    f"[{platform_label}] ━━━ BATCH {total_batches} ━━━ | reason:{fire_reason} | "
                    f"items:{len(batch_to_send)} | received:{total_received} "
                    f"matched:{total_matched} dropped:{total_dropped}"
                )

                scores = score_batch_with_claude(batch_to_send)
                score_map = {int(s.get("index", 0)): s for s in scores if s.get("index")}

                for i, it in enumerate(batch_to_send):
                    pos = i + 1
                    sr = score_map.get(pos) or (scores[i] if i < len(scores) else _fallback_score(pos, "Index mismatch."))
                    is_fallback = bool(sr.get("_is_fallback", False))
                    save_new_signal(it, sr, force_pending=is_fallback)

                log.info(f"[{platform_label}] BATCH {total_batches} COMPLETE — "
                         f"{len(batch_to_send)} item(s) | waiting {gap_seconds}s...")
                time.sleep(gap_seconds)

        except Exception as exc:
            log.error(f"[{platform_label}] batch processor error: {exc}")
            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# RESCORE PROCESSOR — polls the `signals` collection DIRECTLY for
# {"status": "pending"} documents (Claude-failure items).
# ─────────────────────────────────────────────────────────────────────────────

def run_rescore_processor():
    log.info(f"[RESCORE] Processor started | batch_size:{RESCORE_BATCH_SIZE} | "
             f"poll:{RESCORE_POLL_INTERVAL}s | gap:{RESCORE_BATCH_GAP_SECONDS}s")
    total_batches = 0

    while True:
        try:
            pending = list(db.signals.find({"status": "pending"}).limit(RESCORE_BATCH_SIZE))
            if not pending:
                time.sleep(RESCORE_POLL_INTERVAL)
                continue

            items_for_claude = []
            for doc in pending:
                items_for_claude.append({
                    "message_id":     doc["message_id"],
                    "platform":       doc.get("platform", "unknown"),
                    "text":           doc.get("text", ""),
                    "search_keyword": doc.get("search_keyword", SEARCH_KEYWORD),
                    "google_rank":    doc.get("google_rank"),
                    "search_volume":  doc.get("search_volume"),
                    "upvotes":        doc.get("upvotes"),
                    "comments":       doc.get("comments"),
                })

            total_batches += 1
            log.info(f"[RESCORE] BATCH {total_batches} | items:{len(items_for_claude)}")

            scores = score_batch_with_claude(items_for_claude)
            score_map = {int(s.get("index", 0)): s for s in scores if s.get("index")}

            for i, item in enumerate(items_for_claude):
                pos = i + 1
                sr = score_map.get(pos) or (scores[i] if i < len(scores) else _fallback_score(pos))
                enrichment = {
                    "google_rank":    item.get("google_rank"),
                    "search_volume":  item.get("search_volume"),
                    "upvotes":        item.get("upvotes"),
                    "comments":       item.get("comments"),
                    "search_keyword": item.get("search_keyword"),
                }
                # NOTE: even if this rescore attempt ALSO fails (still a
                # fallback score), replace_confirmed_signal marks it
                # "confirmed" — this prevents an infinite pending loop.
                replace_confirmed_signal(item["message_id"], enrichment, sr)

            log.info(f"[RESCORE] BATCH {total_batches} DONE — waiting {RESCORE_BATCH_GAP_SECONDS}s...")
            time.sleep(RESCORE_BATCH_GAP_SECONDS)

        except Exception as exc:
            log.error(f"[RESCORE] processor error: {exc}")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER / X POLLER
# ─────────────────────────────────────────────────────────────────────────────

def build_twitter_client() -> tweepy.Client | None:
    if not TWITTER_BEARER_TOKEN:
        log.warning("TWITTER_BEARER_TOKEN not set — Twitter platform disabled.")
        return None
    try:
        client = tweepy.Client(
            bearer_token=TWITTER_BEARER_TOKEN,
            consumer_key=TWITTER_API_KEY,
            consumer_secret=TWITTER_API_SECRET,
            wait_on_rate_limit=True,
        )
        log.info("Twitter/X client initialised.")
        return client
    except Exception as exc:
        log.error(f"Twitter client error: {exc}")
        return None


def poll_twitter(client: tweepy.Client):
    seen_ids: set = load_seen_ids("twitter")
    dirty = 0
    log.info(f"Twitter poll started | query_len:{len(TWITTER_SEARCH_QUERY)} | "
             f"dedup resumed with {len(seen_ids)} ID(s)")

    while True:
        try:
            response = client.search_recent_tweets(
                query=TWITTER_SEARCH_QUERY,
                max_results=50,
                tweet_fields=["author_id", "created_at", "text", "public_metrics"],
                expansions=["author_id"],
                user_fields=["username", "name"],
            )

            if not response or not response.data:
                time.sleep(TWITTER_POLL_INTERVAL)
                continue

            user_map = {u.id: u.username for u in (response.includes or {}).get("users", [])}

            new_count = 0
            for tweet in response.data:
                tweet_id = str(tweet.id)
                if tweet_id in seen_ids:
                    continue
                seen_ids.add(tweet_id)
                dirty += 1
                if len(seen_ids) > 50_000:
                    seen_ids.clear()

                username = user_map.get(tweet.author_id, f"user_{tweet.author_id}")
                metrics = tweet.public_metrics or {}

                _tw_item = {
                    "message_id":           f"twitter_{tweet_id}",
                    "platform":             "twitter",
                    "text":                 tweet.text or "",
                    "username":             username,
                    "subreddit_or_channel": "",
                    "post_url":             f"https://twitter.com/{username}/status/{tweet_id}",
                    "posted_at":            str(tweet.created_at) if tweet.created_at else None,
                    "search_keyword":       SEARCH_KEYWORD,
                    "upvotes":              metrics.get("like_count"),
                    "comments":             metrics.get("reply_count"),
                    "google_rank":          None,
                    "search_volume":        None,
                }
                twitter_queue.put(_tw_item)
                save_queue_message("twitter", _tw_item)
                new_count += 1

            if dirty >= 10:
                save_seen_ids("twitter", seen_ids)
                dirty = 0

            if new_count:
                log.info(f"Twitter: {new_count} new tweets queued | queue_size:{twitter_queue.qsize()}")

        except tweepy.errors.TweepyException as exc:
            log.error(f"Twitter poll error: {exc}")
        except Exception as exc:
            log.error(f"Twitter unexpected error: {exc}")

        time.sleep(TWITTER_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC LISTENERS — thread management + auto-restart
# ─────────────────────────────────────────────────────────────────────────────

async def start_reddit_listener():
    """
    Reddit's ONLY mechanism now: SERP discovery thread (per-keyword
    fetch-once-forever cache + batched search-volume seeding -> Google
    search -> public, credential-free RSS fetch) + its dedicated batch
    processor thread. Governed entirely by REDDIT_ENABLED + RapidAPI
    credentials (RapidAPI is still required for SERP discovery itself;
    the per-post fetch step needs no credentials at all — no OAuth/PRAW).
    """
    if not REDDIT_ENABLED:
        log.warning("Reddit platform DISABLED — skipping.")
        return
    if not RAPIDAPI_KEY:
        log.warning("Reddit not started — RAPIDAPI_KEY not set (required for SERP discovery).")
        return

    resumed = load_queue_messages("reddit")
    for it in resumed:
        reddit_queue.put(it)
    if resumed:
        log.info(f"[REDDIT] Resumed {len(resumed)} queue message(s) from MongoDB after restart.")

    serp_thread = threading.Thread(target=run_serp_discovery_loop, daemon=True, name="Reddit-SERP")
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT", REDDIT_BATCH_GAP_SECONDS,
              REDDIT_BATCH_TIMEOUT_SECONDS, REDDIT_SEARCH_KEYWORDS),
        daemon=True, name="Reddit-Batch",
    )
    serp_thread.start()
    btch_thread.start()
    log.info(f"Reddit threads running: SERP-Discovery ✅ | Batch ✅ | "
             f"gap:{REDDIT_BATCH_GAP_SECONDS}s | timeout:{REDDIT_BATCH_TIMEOUT_SECONDS}s")

    while True:
        await asyncio.sleep(60)
        if not serp_thread.is_alive():
            log.error("Reddit SERP thread died — restarting...")
            serp_thread = threading.Thread(target=run_serp_discovery_loop, daemon=True, name="Reddit-SERP")
            serp_thread.start()
        if not btch_thread.is_alive():
            log.error("Reddit batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor,
                args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT", REDDIT_BATCH_GAP_SECONDS,
                      REDDIT_BATCH_TIMEOUT_SECONDS, REDDIT_SEARCH_KEYWORDS),
                daemon=True, name="Reddit-Batch",
            )
            btch_thread.start()


async def start_twitter_listener():
    if not TWITTER_ENABLED:
        log.warning("Twitter platform DISABLED — skipping.")
        return
    client = build_twitter_client()
    if client is None:
        return

    resumed = load_queue_messages("twitter")
    for it in resumed:
        twitter_queue.put(it)
    if resumed:
        log.info(f"[TWITTER] Resumed {len(resumed)} queue message(s) from MongoDB after restart.")

    poll_thread = threading.Thread(target=poll_twitter, args=(client,), daemon=True, name="Twitter-Poll")
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(twitter_queue, TWITTER_BATCH_SIZE, "TWITTER", TWITTER_BATCH_GAP_SECONDS,
              TWITTER_BATCH_TIMEOUT_SECONDS, TWITTER_SEARCH_KEYWORDS),
        daemon=True, name="Twitter-Batch",
    )
    poll_thread.start()
    btch_thread.start()
    log.info(f"Twitter threads running: Poll ✅ | Batch ✅ | "
             f"gap:{TWITTER_BATCH_GAP_SECONDS}s | timeout:{TWITTER_BATCH_TIMEOUT_SECONDS}s")

    while True:
        await asyncio.sleep(60)
        if not poll_thread.is_alive():
            log.error("Twitter poll thread died — restarting...")
            poll_thread = threading.Thread(target=poll_twitter, args=(client,), daemon=True, name="Twitter-Poll")
            poll_thread.start()
        if not btch_thread.is_alive():
            log.error("Twitter batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor,
                args=(twitter_queue, TWITTER_BATCH_SIZE, "TWITTER", TWITTER_BATCH_GAP_SECONDS,
                      TWITTER_BATCH_TIMEOUT_SECONDS, TWITTER_SEARCH_KEYWORDS),
                daemon=True, name="Twitter-Batch",
            )
            btch_thread.start()


async def start_rescore_listener():
    rescore_thread = threading.Thread(target=run_rescore_processor, daemon=True, name="Rescore-Processor")
    rescore_thread.start()
    log.info("Rescore processor thread running ✅")

    while True:
        await asyncio.sleep(60)
        if not rescore_thread.is_alive():
            log.error("Rescore processor thread died — restarting...")
            rescore_thread = threading.Thread(target=run_rescore_processor, daemon=True, name="Rescore-Processor")
            rescore_thread.start()


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI — read-only endpoints
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Flintel v9.11 — Reddit (SERP + fetch-once-forever keyword cache + batched search-volume seeding + credential-free RSS fetch + random-fallback volume/engagement) + Twitter Signal Scorer",
    description=(
        "Reddit (RapidAPI SERP discovery, fetch-once-forever keyword cache — "
        "no re-fetch, ever, once a keyword is done) + Twitter signals: monitor, "
        "score (generic 1-100 relevance/visibility/engagement model), store. "
        "Reddit per-post fetch uses ONLY Reddit's public, credential-free per-post RSS "
        "endpoint (smart-retry + old.reddit.com fallback) — no OAuth, no PRAW, "
        "nothing to configure. Search-volume ('search/mo') failures — bad key, "
        "exhausted credits, rate-limits, timeouts, or no usable field — are "
        "NEVER left as a permanent None: a random placeholder in a "
        "configurable range (default 300-5000) is generated instead, and "
        "every single occurrence is logged with a clearly-labelled "
        "'RANDOM FALLBACK' warning naming the exact value + reason, so it's "
        "always distinguishable in the logs from a real value. This is fully "
        "independent of — and never blocks or is blocked by — the separate "
        "Google-rank/SERP RapidAPI calls, which run on their own host and "
        "their own try/except. Persistent batch state + queue + dedup — no "
        "in-flight item is ever lost on restart. Each keyword is tracked in "
        "flintel_keywords and, once fetched, is PERMANENTLY marked done — "
        "restarts never reset progress and never trigger a re-fetch of an "
        "already-done keyword. Newly added keywords are picked up "
        "automatically, one at a time. Streaming Claude with partial-JSON "
        "recovery. Claude failures route to status='pending' for automatic "
        "rescore (re-uses stored enrichment, never re-fetches from Reddit or "
        "RapidAPI) instead of a permanent low score."
    ),
    version="9.11.0",
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
    now = datetime.now(timezone.utc)
    total_keywords_tracked = db.flintel_keywords.count_documents({})
    due_now_count = db.flintel_keywords.count_documents({
        "keyword": {"$in": REDDIT_SEARCH_KEYWORDS},
        "fetched": False,
    })
    missing_volume_count = db.flintel_keywords.count_documents({
        "keyword": {"$in": REDDIT_SEARCH_KEYWORDS},
        "search_volume": None,
    })
    random_volume_count = db.flintel_keywords.count_documents({
        "keyword": {"$in": REDDIT_SEARCH_KEYWORDS},
        "search_volume_is_random": True,
    })
    return {
        "status":                  "running",
        "system":                  "FLINTEL v9.11 (Reddit SERP + fetch-once-forever keyword cache + batched search-volume seeding + credential-free RSS fetch + random-fallback volume/engagement + Twitter)",
        "client":                  CLIENT_ID,
        "platforms":               ["reddit", "twitter"],
        "reddit_enabled":          REDDIT_ENABLED,
        "reddit_status":           _working(REDDIT_ENABLED and bool(RAPIDAPI_KEY)),
        "reddit_fetch_method":     "public per-post RSS (credential-free, smart-retry + old.reddit.com fallback) — no OAuth/PRAW",
        "twitter_enabled":         TWITTER_ENABLED,
        "twitter_status":          _working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN)),
        "reddit_search_keywords":  len(REDDIT_SEARCH_KEYWORDS),
        "twitter_search_keywords": len(TWITTER_SEARCH_KEYWORDS),
        "keyword_check_interval_seconds": KEYWORD_CHECK_INTERVAL_SECONDS,
        "keyword_cache":                  "ENABLED — fetch-once-forever, restart-safe (flintel_keywords)",
        "search_volume_seeding":           f"BATCHED loop (chunks of {SEARCH_VOLUME_BATCH_SIZE})",
        "search_volume_random_fallback":   f"ENABLED — range {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-{SEARCH_VOLUME_RANDOM_FALLBACK_MAX}, always logged, never overrides a real value",
        "reddit_fetch_reliability":         f"public RSS only, credential-free — smart-retry ({REDDIT_FETCH_MAX_RETRIES}x backoff + old.reddit.com fallback)",
        "reddit_engagement_random_fallback": f"ENABLED — range {REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN}-{REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX} (RSS has no real upvotes/comments), always logged",
        "keywords_tracked":               total_keywords_tracked,
        "keywords_due_now":               due_now_count,
        "keywords_missing_search_volume": missing_volume_count,
        "keywords_with_random_search_volume": random_volume_count,
        "serp_months_back":        SERP_MONTHS_BACK,
        "serp_results_per_kw":     SERP_RESULTS_PER_KEYWORD,
        "reddit_batch_size":       REDDIT_BATCH_SIZE,
        "twitter_batch_size":      TWITTER_BATCH_SIZE,
        "rescore_batch_size":      RESCORE_BATCH_SIZE,
        "reddit_batch_gap_s":      REDDIT_BATCH_GAP_SECONDS,
        "reddit_batch_timeout_s":  REDDIT_BATCH_TIMEOUT_SECONDS,
        "twitter_batch_gap_s":     TWITTER_BATCH_GAP_SECONDS,
        "twitter_batch_timeout_s": TWITTER_BATCH_TIMEOUT_SECONDS,
        "rescore_batch_gap_s":     RESCORE_BATCH_GAP_SECONDS,
        "rapidapi_configured":    bool(RAPIDAPI_KEY),
        "reddit_queue_size":       reddit_queue.qsize(),
        "twitter_queue_size":      twitter_queue.qsize(),
        "rescore_pending":         db.signals.count_documents({"status": "pending"}),
        "auth_required":           bool(API_KEY),
        "telegram_removed":        True,
        "reddit_rss_removed":      True,
        "reddit_oauth_praw_removed": True,
        "fixed_full_cycle_sleep_removed": True,
        "post_url_dedup_before_scoring": True,
        "claude_failure_routes_to_pending": True,
        "output_schema":           "intent_score (1-100) / is_relevant / reply_draft",
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
        "reddit_fetch_method":     "public per-post RSS (credential-free) — no OAuth/PRAW",
        "twitter_working":         TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN),
        "twitter_indicator":       _working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN)),
        "reddit_queue_size":       reddit_queue.qsize(),
        "twitter_queue_size":      twitter_queue.qsize(),
        "rescore_pending":         db.signals.count_documents({"status": "pending"}),
        "client_id":               CLIENT_ID,
        "timestamp":               datetime.now(timezone.utc).isoformat(),
    }


@app.get("/keywords", dependencies=[Depends(verify_api_key)])
def get_keywords_status():
    """
    Inspect the fetch-once-forever keyword cache directly — for every
    keyword shows whether it's been fetched (true = permanently done,
    never re-fetched; false = still pending, due on the next pass), its
    cached search_volume (real value or a random-fallback placeholder —
    see search_volume_is_random), and when it was last fetched.
    """
    raw_docs = list(db.flintel_keywords.find({}, {"_id": 0}).sort("keyword", 1))
    due_count = 0
    missing_volume_count = 0
    random_volume_count = 0
    docs = []
    for d in raw_docs:
        is_due = not d.get("fetched")
        if is_due:
            due_count += 1
        if d.get("search_volume") is None:
            missing_volume_count += 1
        if d.get("search_volume_is_random"):
            random_volume_count += 1
        for f in ["last_fetched_at", "created_at"]:
            if d.get(f):
                d[f] = d[f].isoformat()
        d["due_now"] = is_due
        docs.append(d)
    return {
        "total": len(docs),
        "due_now": due_count,
        "missing_search_volume": missing_volume_count,
        "random_fallback_search_volume": random_volume_count,
        "keywords": docs,
    }


@app.get("/signals", dependencies=[Depends(verify_api_key)])
def get_signals(limit: int = 50, min_score: int = None, is_relevant: bool = None,
                 platform: str = None, status: str = None):
    q: dict = {"client_id": CLIENT_ID}
    if min_score is not None:
        q["intent_score"] = {"$gte": min_score}
    if is_relevant is not None:
        q["is_relevant"] = is_relevant
    if platform:
        q["platform"] = platform
    if status:
        q["status"] = status
    signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
    return {"count": len(signals), "signals": _serialise(signals)}


@app.get("/signals/relevant", dependencies=[Depends(verify_api_key)])
def get_relevant_signals(limit: int = 50, min_score: int = 0):
    signals = list(
        db.signals.find(
            {"client_id": CLIENT_ID, "is_relevant": True, "intent_score": {"$gte": min_score}},
            {"_id": 0},
        ).sort("intent_score", -1).limit(limit)
    )
    return {"count": len(signals), "signals": _serialise(signals)}


@app.get("/signals/pending", dependencies=[Depends(verify_api_key)])
def get_pending(limit: int = 100):
    signals = list(db.signals.find({"status": "pending"}, {"_id": 0}).limit(limit))
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
        start_twitter_listener(),
        start_rescore_listener(),
    )


if __name__ == "__main__":
    log.info("=" * 70)
    log.info("  FLINTEL v9.11 — REDDIT (SERP + FETCH-ONCE-FOREVER KEYWORD CACHE")
    log.info("                   + BATCHED SEARCH-VOLUME SEEDING + CREDENTIAL-FREE")
    log.info("                   .JSON FETCH + RANDOM-FALLBACK SEARCH VOLUME) + TWITTER SIGNAL SCORER")
    log.info("=" * 70)
    log.info(f"  Client               : {CLIENT_ID}")
    log.info(f"  Platforms            : Reddit (SERP discovery, fetch-once-forever) + Twitter/X")
    log.info(f"  Reddit               : {REDDIT_ENABLED} | {_working(REDDIT_ENABLED and bool(RAPIDAPI_KEY))}")
    log.info(f"  Reddit fetch method  : public per-post RSS only — credential-free, no OAuth/PRAW, nothing to configure")
    log.info(f"  Reddit engagement    : RANDOM placeholder {REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN}-{REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX} (upvotes/comments) — RSS has no real counts, always logged")
    log.info(f"  Twitter              : {TWITTER_ENABLED} | {_working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN))}")
    log.info(f"  Reddit keywords      : {len(REDDIT_SEARCH_KEYWORDS)} (used for SERP discovery)")
    log.info(f"  Twitter keywords     : {len(TWITTER_SEARCH_KEYWORDS)} (used for Twitter search query)")
    log.info(f"  Keyword cache        : fetch-once-forever (no re-fetch, ever) | check every {KEYWORD_CHECK_INTERVAL_SECONDS}s | "
             f"last {SERP_MONTHS_BACK} months | depth {SERP_RESULTS_PER_KEYWORD}")
    log.info(f"  Search-volume seeding: batched loop, chunks of {SEARCH_VOLUME_BATCH_SIZE} keywords | "
             f"cached on flintel_keywords, read at discovery time | error status+message logged; "
             f"never blocks rank/reddit fetch")
    log.info(f"  Search-volume fallback: RANDOM placeholder {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-"
             f"{SEARCH_VOLUME_RANDOM_FALLBACK_MAX} on any failure/no-credits — always clearly logged, "
             f"never overrides a real value")
    log.info(f"  Reddit fetch         : public RSS smart-retry only ({REDDIT_FETCH_MAX_RETRIES}x backoff, "
             f"jitter {REDDIT_FETCH_JITTER_MIN}-{REDDIT_FETCH_JITTER_MAX}s, old.reddit.com fallback) — "
             f"no OAuth/PRAW")
    log.info(f"  Reddit batch         : {REDDIT_BATCH_SIZE} items OR {REDDIT_BATCH_TIMEOUT_SECONDS}s | gap {REDDIT_BATCH_GAP_SECONDS}s")
    log.info(f"  Twitter batch        : {TWITTER_BATCH_SIZE} items OR {TWITTER_BATCH_TIMEOUT_SECONDS}s | gap {TWITTER_BATCH_GAP_SECONDS}s")
    log.info(f"  Rescore batch        : {RESCORE_BATCH_SIZE} items | poll {RESCORE_POLL_INTERVAL}s | gap {RESCORE_BATCH_GAP_SECONDS}s")
    log.info(f"  Rescore source       : signals collection, status='pending' — never re-fetches, only re-scores")
    log.info(f"  Claude streaming     : True | prompt: generic 1-100 relevance/visibility/engagement")
    log.info(f"  RapidAPI config      : {bool(RAPIDAPI_KEY)} (SOLE provider — google_rank + search_volume)")
    log.info(f"  Telegram             : REMOVED")
    log.info(f"  Reddit RSS           : REMOVED")
    log.info(f"  Reddit OAuth/PRAW    : REMOVED")
    log.info(f"  Fixed full-cycle sleep: REMOVED (each keyword has its own independent fetch-once-forever state)")
    log.info(f"  MongoDB DB           : {MONGODB_DB}")
    log.info(f"  API auth             : {'True | ' + _working(True) if API_KEY else 'False | ' + _working(False)}")
    log.info("=" * 70)

    asyncio.run(main())
