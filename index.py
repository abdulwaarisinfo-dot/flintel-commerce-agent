"""
FLINTEL v9.12 — Reddit (SERP Discovery, FETCH-ONCE-FOREVER KEYWORD CACHE
                + BATCHED SEARCH-VOLUME PRE-SEEDING)
                + Twitter/X Signal Scorer
=================================================================================
Platforms : Reddit — RapidAPI SERP discovery ONLY (Google search,
            site:reddit.com, real per-post rank -> flintel_google_posts cache
            -> subreddit RSS polling + fuzzy-keyword + URL-match confirmation,
            no credentials required)
          + Twitter/X (tweepy v2)

=================================================================================
WHAT CHANGED IN THIS BUILD (v9.12) — REDDIT PER-POST FETCH DECOUPLED FROM
SERP DISCOVERY VIA A NEW flintel_google_posts CACHE + AUTO-GENERATED FUZZY
KEYWORDS + SUBREDDIT-RSS MATCHING. flintel_keywords, THE SEARCH-VOLUME
SEEDING LOGIC, AND THE GOOGLE-RANK/SERP CALL ARE 100% UNTOUCHED.
=================================================================================

  ISSUE (confirmed, not a code bug elsewhere): Reddit's public, anonymous
    per-post RSS fetch (post_url + ".rss") was called SYNCHRONOUSLY right
    inside SERP discovery (process_one_keyword()) for every single
    discovered post_url, one at a time, immediately after the Google SERP
    call returned. This meant: (a) SERP discovery (which is cheap and
    reliable — its own RapidAPI host) was throttled by however slow/
    unreliable Reddit's anonymous RSS endpoint happened to be for a given
    IP that day, and (b) a keyword could only be marked fetched=True once
    EVERY discovered post's Reddit RSS fetch had been attempted, tying the
    keyword-cache's "done" state to Reddit's fetch reliability instead of
    to the SERP discovery step alone.

  FIX — Reddit per-post fetching is now fully decoupled from SERP
    discovery via a NEW collection, flintel_google_posts:

      1. process_one_keyword() (SERP discovery) NO LONGER calls
         fetch_reddit_post_by_url() at all. Instead, for every Google SERP
         result belonging to a due keyword, it:
           - extracts the subreddit name from the post_url
           - auto-generates 6-7 "fuzzy keywords" from the matched Google
             search keyword (deterministic, smart word-combination based —
             see generate_fuzzy_keywords()) so a later RSS-based matching
             pass can recognize related posts by text even without
             depending on Reddit's per-post endpoint
           - stores ONE document in flintel_google_posts:
               {post_url, google_rank, matched_keyword, fuzzy_keywords,
                subreddit, fetched: False, created_at}
         This save happens immediately and does NOT wait on any Reddit
         fetch — SERP discovery's throughput is now independent of
         Reddit's RSS reliability entirely.

      2. A brand-new, fully independent background thread
         (run_google_posts_rss_matching_loop()) is the ONLY place that
         talks to Reddit's RSS feeds now. It:
           - reads flintel_google_posts directly (NOT any python list) for
             every DISTINCT subreddit that still has fetched=False
             documents
           - polls that subreddit's public, credential-free
             https://www.reddit.com/r/<subreddit>/new.rss feed (same
             smart-retry + old.reddit.com fallback host logic as before)
           - for every RSS entry, checks it against the pending
             flintel_google_posts documents for that subreddit by exact
             post_url match (the authoritative signal — we already know
             this exact URL was discovered via SERP) and cross-references
             the entry's title/text against that document's stored
             fuzzy_keywords purely for extra traceability in the logs
           - the instant a post_url match is found: pulls that keyword's
             already-seeded search_volume straight off flintel_keywords
             (read-only — NEVER re-queries the search-volume API here),
             builds the exact same item schema as before (message_id,
             platform, text, username, subreddit_or_channel, post_url,
             posted_at, search_keyword, upvotes, comments,
             engagement_is_random, google_rank, search_volume,
             search_volume_is_random), pushes it into reddit_queue +
             save_queue_message(), and marks that flintel_google_posts
             document fetched=True — permanently, same fetch-once-forever
             spirit as flintel_keywords.

    flintel_keywords is NEVER written to by this new loop except for the
    existing read-only search_volume lookup — sync_keywords_to_db(),
    get_due_keywords(), get_keywords_missing_volume(),
    mark_keyword_fetched(), set_keyword_retry_cooldown(),
    seed_search_volume_batch(), search_google_for_keyword(),
    fetch_google_rank(), fetch_search_volume() are BYTE-FOR-BYTE UNCHANGED
    from v9.11.1. process_one_keyword() itself keeps marking a keyword
    fetched=True once its SERP results have all been saved to
    flintel_google_posts — that marking no longer depends on Reddit RSS
    succeeding at all, since Reddit RSS fetching is now a fully separate,
    later, independent stage.

    Subreddits/keywords/fuzzy-keywords are NEVER stored in a python list
    for this new stage — run_google_posts_rss_matching_loop() reads
    flintel_google_posts directly, every single pass, exactly the way
    flintel_keywords is read directly (no REDDIT_SEARCH_KEYWORDS-style
    python list involved for this part at all).
=================================================================================
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
# Reddit's public RSS feed (used for the per-post fetch — see module
# docstring) does NOT expose numeric upvote or comment counts — this is a
# genuine schema limitation of the RSS format itself, not a parsing bug.
# Since Component 3 (Engagement Signal) of the Claude scoring model needs
# a numeric value to score against, every Reddit post confirmed via RSS
# gets a random placeholder upvotes/comments value in this range instead
# of None/0, using the exact same "random fallback, always logged, never
# silently indistinguishable from a real value" pattern already used for
# search_volume above.
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
#     This looks at ALL of flintel_keywords, not just whatever happens to
#     still be in this python list right now.
#   - get_due_keywords() picks up only fetched=False keywords — looks at
#     ALL of flintel_keywords, not just this python list.
#   - mark_keyword_fetched() flips a keyword to fetched=True PERMANENTLY
#     right after its SERP results are all saved to flintel_google_posts
#     — it will never be re-fetched.
REDDIT_SEARCH_KEYWORDS = [
     "best ai agent for startups",
    "best ai agent for small business",
    "best ai agent for saas",
    "best ai agent for agencies",
    "best ai agent for ecommerce",
    "best ai agent for sales teams",
    "best ai agent for b2b",
    "best ai agent for enterprises",
    "best ai agent for customer support",
    "best ai agent for marketing teams",
    "best ai agent for real estate",
    "best ai agent for recruiters",
    "best ai agent for hr teams",
    "best ai agent for healthcare",
    "best ai agent for finance teams",
    "best ai agent for travel companies",
    "best ai agent for developers",
    "best ai agent for local businesses",
    "best ai agent for startups 2026",
    "best ai agent in 2026",
    "best ai agent for remote teams",
    "top ai agent for startups",
    "top ai agent for small business",
    "top ai agent for saas",
    "top ai agent for agencies",
    "top ai agent for ecommerce",
    "top ai agent for sales teams",
    "top ai agent for b2b",
    "top ai agent for enterprises",
    "top ai agent for customer support",
    "top ai agent for marketing teams",
    "top ai agent for real estate",
    "top ai agent for recruiters",
    "top ai agent for hr teams",
    "top ai agent for healthcare",
    "top ai agent for finance teams",
    "top ai agent for travel companies",
    "top ai agent for developers",
    "top ai agent for local businesses",
    "top ai agent for startups 2026",
    "top ai agent in 2026",
    "top ai agent for remote teams",
    "recommended ai agent for startups",
    "recommended ai agent for small business",
    "recommended ai agent for saas",
    "recommended ai agent for agencies",
    "recommended ai agent for ecommerce",
    "recommended ai agent for sales teams",
    "recommended ai agent for b2b",
    "recommended ai agent for enterprises",
    "recommended ai agent for customer support",
    "recommended ai agent for marketing teams",
    "recommended ai agent for real estate",
    "recommended ai agent for recruiters",
    "recommended ai agent for hr teams",
    "recommended ai agent for healthcare",
    "recommended ai agent for finance teams",
    "recommended ai agent for travel companies",
    "recommended ai agent for developers",
    "recommended ai agent for local businesses",
    "recommended ai agent for startups 2026",
    "recommended ai agent in 2026",
    "recommended ai agent for remote teams",
    "affordable ai agent for startups",
    "affordable ai agent for small business",
    "affordable ai agent for saas",
    "affordable ai agent for agencies",
    "affordable ai agent for ecommerce",
    "affordable ai agent for sales teams",
    "affordable ai agent for b2b",
    "affordable ai agent for enterprises",
    "affordable ai agent for customer support",
    "affordable ai agent for marketing teams",
    "affordable ai agent for real estate",
    "affordable ai agent for recruiters",
    "affordable ai agent for hr teams",
    "affordable ai agent for healthcare",
    "affordable ai agent for finance teams",
    "affordable ai agent for travel companies",
    "affordable ai agent for developers",
    "affordable ai agent for local businesses",
    "affordable ai agent for startups 2026",
    "affordable ai agent in 2026",
    "affordable ai agent for remote teams",
    "enterprise ai agent for startups",
    "enterprise ai agent for small business",
    "enterprise ai agent for saas",
    "enterprise ai agent for agencies",
    "enterprise ai agent for ecommerce",
    "enterprise ai agent for sales teams",
    "enterprise ai agent for b2b",
    "enterprise ai agent for enterprises",
    "enterprise ai agent for customer support",
    "enterprise ai agent for marketing teams",
    "enterprise ai agent for real estate",
    "enterprise ai agent for recruiters",
    "enterprise ai agent for hr teams",
    "enterprise ai agent for healthcare",
    "enterprise ai agent for finance teams",
    "enterprise ai agent for travel companies",
    "enterprise ai agent for developers",
    "enterprise ai agent for local businesses",
    "enterprise ai agent for startups 2026",
    "enterprise ai agent in 2026",
    "enterprise ai agent for remote teams",
    "small business ai agent for startups",
    "small business ai agent for small business",
    "small business ai agent for saas",
    "small business ai agent for agencies",
    "small business ai agent for ecommerce",
    "small business ai agent for sales teams",
    "small business ai agent for b2b",
    "small business ai agent for enterprises",
    "small business ai agent for customer support",
    "small business ai agent for marketing teams",
    "small business ai agent for real estate",
    "small business ai agent for recruiters",
    "small business ai agent for hr teams",
    "small business ai agent for healthcare",
    "small business ai agent for finance teams",
    "small business ai agent for travel companies",
    "small business ai agent for developers",
    "small business ai agent for local businesses",
    "small business ai agent for startups 2026",
    "small business ai agent in 2026",
    "small business ai agent for remote teams",
    "startup ai agent for startups",
    "startup ai agent for small business",
    "startup ai agent for saas",
    "startup ai agent for agencies",
    "startup ai agent for ecommerce",
    "startup ai agent for sales teams",
    "startup ai agent for b2b",
    "startup ai agent for enterprises",
    "startup ai agent for customer support",
    "startup ai agent for marketing teams",
    "startup ai agent for real estate",
    "startup ai agent for recruiters",
    "startup ai agent for hr teams",
    "startup ai agent for healthcare",
    "startup ai agent for finance teams",
    "startup ai agent for travel companies",
    "startup ai agent for developers",
    "startup ai agent for local businesses",
    "startup ai agent for startups 2026",
    "startup ai agent in 2026",
    "startup ai agent for remote teams",
    "alternative ai agent for startups",
    "alternative ai agent for small business",
    "alternative ai agent for saas",
    "alternative ai agent for agencies",
    "alternative ai agent for ecommerce",
    "alternative ai agent for sales teams",
    "alternative ai agent for b2b",
    "alternative ai agent for enterprises",
    "alternative ai agent for customer support",
    "alternative ai agent for marketing teams",
    "alternative ai agent for real estate",
    "alternative ai agent for recruiters",
    "alternative ai agent for hr teams",
    "alternative ai agent for healthcare",
    "alternative ai agent for finance teams",
    "alternative ai agent for travel companies",
    "alternative ai agent for developers",
    "alternative ai agent for local businesses",
    "alternative ai agent for startups 2026",
    "alternative ai agent in 2026",
    "alternative ai agent for remote teams",
    "alternatives ai agent for startups",
    "alternatives ai agent for small business",
    "alternatives ai agent for saas",
    "alternatives ai agent for agencies",
    "alternatives ai agent for ecommerce",
    "alternatives ai agent for sales teams",
    "alternatives ai agent for b2b",
    "alternatives ai agent for enterprises",
    "alternatives ai agent for customer support",
    "alternatives ai agent for marketing teams",
    "alternatives ai agent for real estate",
    "alternatives ai agent for recruiters",
    "alternatives ai agent for hr teams",
    "alternatives ai agent for healthcare",
    "alternatives ai agent for finance teams",
    "alternatives ai agent for travel companies",
    "alternatives ai agent for developers",
    "alternatives ai agent for local businesses",
    "alternatives ai agent for startups 2026",
    "alternatives ai agent in 2026",
    "alternatives ai agent for remote teams",
    "comparison ai agent for startups",
    "comparison ai agent for small business",
    "comparison ai agent for saas",
    "comparison ai agent for agencies",
    "comparison ai agent for ecommerce",
    "comparison ai agent for sales teams",
    "comparison ai agent for b2b",
    "comparison ai agent for enterprises",
    "comparison ai agent for customer support",
    "comparison ai agent for marketing teams",
    "comparison ai agent for real estate",
    "comparison ai agent for recruiters",
    "comparison ai agent for hr teams",
    "comparison ai agent for healthcare",
    "comparison ai agent for finance teams",
    "comparison ai agent for travel companies",
    "comparison ai agent for developers",
    "comparison ai agent for local businesses",
    "comparison ai agent for startups 2026",
    "comparison ai agent in 2026",
    "comparison ai agent for remote teams",
    "vs ai agent for startups",
    "vs ai agent for small business",
    "vs ai agent for saas",
    "vs ai agent for agencies",
    "vs ai agent for ecommerce",
    "vs ai agent for sales teams",
    "vs ai agent for b2b",
    "vs ai agent for enterprises",
    "vs ai agent for customer support",
    "vs ai agent for marketing teams",
    "vs ai agent for real estate",
    "vs ai agent for recruiters",
    "vs ai agent for hr teams",
    "vs ai agent for healthcare",
    "vs ai agent for finance teams",
    "vs ai agent for travel companies",
    "vs ai agent for developers",
    "vs ai agent for local businesses",
    "vs ai agent for startups 2026",
    "vs ai agent in 2026",
    "vs ai agent for remote teams",
    "pricing ai agent for startups",
    "pricing ai agent for small business",
    "pricing ai agent for saas",
    "pricing ai agent for agencies",
    "pricing ai agent for ecommerce",
    "pricing ai agent for sales teams",
    "pricing ai agent for b2b",
    "pricing ai agent for enterprises",
    "pricing ai agent for customer support",
    "pricing ai agent for marketing teams",
    "pricing ai agent for real estate",
    "pricing ai agent for recruiters",
    "pricing ai agent for hr teams",
    "pricing ai agent for healthcare",
    "pricing ai agent for finance teams",
    "pricing ai agent for travel companies",
    "pricing ai agent for developers",
    "pricing ai agent for local businesses",
    "pricing ai agent for startups 2026",
    "pricing ai agent in 2026",
    "pricing ai agent for remote teams",
    "review ai agent for startups",
    "review ai agent for small business",
    "review ai agent for saas",
    "review ai agent for agencies",
    "review ai agent for ecommerce",
    "review ai agent for sales teams",
    "review ai agent for b2b",
    "review ai agent for enterprises",
    "review ai agent for customer support",
    "review ai agent for marketing teams",
    "review ai agent for real estate",
    "review ai agent for recruiters",
    "review ai agent for hr teams",
    "review ai agent for healthcare",
    "review ai agent for finance teams",
    "review ai agent for travel companies",
    "review ai agent for developers",
    "review ai agent for local businesses",
    "review ai agent for startups 2026",
    "review ai agent in 2026",
    "review ai agent for remote teams",
    "software ai agent for startups",
    "software ai agent for small business",
    "software ai agent for saas",
    "software ai agent for agencies",
    "software ai agent for ecommerce",
    "software ai agent for sales teams",
    "software ai agent for b2b",
    "software ai agent for enterprises",
    "software ai agent for customer support",
    "software ai agent for marketing teams",
    "software ai agent for real estate",
    "software ai agent for recruiters",
    "software ai agent for hr teams",
    "software ai agent for healthcare",
    "software ai agent for finance teams",
    "software ai agent for travel companies",
    "software ai agent for developers",
    "software ai agent for local businesses",
    "software ai agent for startups 2026",
    "software ai agent in 2026",
    "software ai agent for remote teams",
    "platform ai agent for startups",
    "platform ai agent for small business",
    "platform ai agent for saas",
    "platform ai agent for agencies",
    "platform ai agent for ecommerce",
    "platform ai agent for sales teams",
    "platform ai agent for b2b",
    "platform ai agent for enterprises",
    "platform ai agent for customer support",
    "platform ai agent for marketing teams",
    "platform ai agent for real estate",
    "platform ai agent for recruiters",
    "platform ai agent for hr teams",
    "platform ai agent for healthcare",
    "platform ai agent for finance teams",
    "platform ai agent for travel companies",
    "platform ai agent for developers",
    "platform ai agent for local businesses",
    "platform ai agent for startups 2026",
    "platform ai agent in 2026",
    "platform ai agent for remote teams",
    "solution ai agent for startups",
    "solution ai agent for small business",
    "solution ai agent for saas",
    "solution ai agent for agencies",
    "solution ai agent for ecommerce",
    "solution ai agent for sales teams",
    "solution ai agent for b2b",
    "solution ai agent for enterprises",
    "solution ai agent for customer support",
    "solution ai agent for marketing teams",
    "solution ai agent for real estate",
    "solution ai agent for recruiters",
    "solution ai agent for hr teams",
    "solution ai agent for healthcare",
    "solution ai agent for finance teams",
    "solution ai agent for travel companies",
    "solution ai agent for developers",
    "solution ai agent for local businesses",
    "solution ai agent for startups 2026",
    "solution ai agent in 2026",
    "solution ai agent for remote teams",
    "tool ai agent for startups",
    "tool ai agent for small business",
    "tool ai agent for saas",
    "tool ai agent for agencies",
    "tool ai agent for ecommerce",
    "tool ai agent for sales teams",
    "tool ai agent for b2b",
    "tool ai agent for enterprises",
    "tool ai agent for customer support",
    "tool ai agent for marketing teams",
    "tool ai agent for real estate",
    "tool ai agent for recruiters",
    "tool ai agent for hr teams",
    "tool ai agent for healthcare",
    "tool ai agent for finance teams",
    "tool ai agent for travel companies",
    "tool ai agent for developers",
    "tool ai agent for local businesses",
    "tool ai agent for startups 2026",
    "tool ai agent in 2026",
    "tool ai agent for remote teams",
    "tools ai agent for startups",
    "tools ai agent for small business",
    "tools ai agent for saas",
    "tools ai agent for agencies",
    "tools ai agent for ecommerce",
    "tools ai agent for sales teams",
    "tools ai agent for b2b",
    "tools ai agent for enterprises",
    "tools ai agent for customer support",
    "tools ai agent for marketing teams",
    "tools ai agent for real estate",
    "tools ai agent for recruiters",
    "tools ai agent for hr teams",
    "tools ai agent for healthcare",
    "tools ai agent for finance teams",
    "tools ai agent for travel companies",
    "tools ai agent for developers",
    "tools ai agent for local businesses",
    "tools ai agent for startups 2026",
    "tools ai agent in 2026",
    "tools ai agent for remote teams",
    "provider ai agent for startups",
    "provider ai agent for small business",
    "provider ai agent for saas",
    "provider ai agent for agencies",
    "provider ai agent for ecommerce",
    "provider ai agent for sales teams",
    "provider ai agent for b2b",
    "provider ai agent for enterprises",
    "provider ai agent for customer support",
    "provider ai agent for marketing teams",
    "provider ai agent for real estate",
    "provider ai agent for recruiters",
    "provider ai agent for hr teams",
    "provider ai agent for healthcare",
    "provider ai agent for finance teams",
    "provider ai agent for travel companies",
    "provider ai agent for developers",
    "provider ai agent for local businesses",
    "provider ai agent for startups 2026",
    "provider ai agent in 2026",
    "provider ai agent for remote teams",
    "service ai agent for startups",
    "service ai agent for small business",
    "service ai agent for saas",
    "service ai agent for agencies",
    "service ai agent for ecommerce",
    "service ai agent for sales teams",
    "service ai agent for b2b",
    "service ai agent for enterprises",
    "service ai agent for customer support",
    "service ai agent for marketing teams",
    "service ai agent for real estate",
    "service ai agent for recruiters",
    "service ai agent for hr teams",
    "service ai agent for healthcare",
    "service ai agent for finance teams",
    "service ai agent for travel companies",
    "service ai agent for developers",
    "service ai agent for local businesses",
    "service ai agent for startups 2026",
    "service ai agent in 2026",
    "service ai agent for remote teams",
    "automation ai agent for startups",
    "automation ai agent for small business",
    "automation ai agent for saas",
    "automation ai agent for agencies",
    "automation ai agent for ecommerce",
    "automation ai agent for sales teams",
    "automation ai agent for b2b",
    "automation ai agent for enterprises",
    "automation ai agent for customer support",
    "automation ai agent for marketing teams",
    "automation ai agent for real estate",
    "automation ai agent for recruiters",
    "automation ai agent for hr teams",
    "automation ai agent for healthcare",
    "automation ai agent for finance teams",
    "automation ai agent for travel companies",
    "automation ai agent for developers",
    "automation ai agent for local businesses",
    "automation ai agent for startups 2026",
    "automation ai agent in 2026",
    "automation ai agent for remote teams",
    "best ai agent",
    "how to use ai agent",
    "how to choose ai agent",
    "looking for ai agent",
    "ai agent recommendations",
    "ai agent alternatives",
    "ai agent comparison",
    "ai agent pricing",
    "ai agent review",
    "ai agent for lead generation",
    "ai agent for customer support",
    "ai agent for sales",
    "ai agent for marketing",
    "ai agent for business automation",
    "ai agent for appointment booking",
    "ai agent for outbound outreach",
    "ai agent for ecommerce",
    "ai agent for workflow automation",
    "ai agent for research",
    "ai agent for business",
    "need ai agent",
    "recommend a ai agent",
    "what is the best ai agent",
    "which ai agent should i use",
    "best ai automation for startups",
    "best ai automation for small business",
    "best ai automation for saas",
    "best ai automation for agencies",
    "best ai automation for ecommerce",
    "best ai automation for sales teams",
    "best ai automation for b2b",
    "best ai automation for enterprises",
    "best ai automation for customer support",
    "best ai automation for marketing teams",
    "best ai automation for real estate",
    "best ai automation for recruiters",
    "best ai automation for hr teams",
    "best ai automation for healthcare",
    "best ai automation for finance teams",
    "best ai automation for travel companies",
    "best ai automation for developers",
    "best ai automation for local businesses",
    "best ai automation for startups 2026",
    "best ai automation in 2026",
    "best ai automation for remote teams",
    "top ai automation for startups",
    "top ai automation for small business",
    "top ai automation for saas",
    "top ai automation for agencies",
    "top ai automation for ecommerce",
    "top ai automation for sales teams",
    "top ai automation for b2b",
    "top ai automation for enterprises",
    "top ai automation for customer support",
    "top ai automation for marketing teams",
    "top ai automation for real estate",
    "top ai automation for recruiters",
    "top ai automation for hr teams",
    "top ai automation for healthcare",
    "top ai automation for finance teams",
    "top ai automation for travel companies",
    "top ai automation for developers",
    "top ai automation for local businesses",
    "top ai automation for startups 2026",
    "top ai automation in 2026",
    "top ai automation for remote teams",
    "recommended ai automation for startups",
    "recommended ai automation for small business",
    "recommended ai automation for saas",
    "recommended ai automation for agencies",
    "recommended ai automation for ecommerce",
    "recommended ai automation for sales teams",
    "recommended ai automation for b2b",
    "recommended ai automation for enterprises",
    "recommended ai automation for customer support",
    "recommended ai automation for marketing teams",
    "recommended ai automation for real estate",
    "recommended ai automation for recruiters",
    "recommended ai automation for hr teams",
    "recommended ai automation for healthcare",
    "recommended ai automation for finance teams",
    "recommended ai automation for travel companies",
    "recommended ai automation for developers",
    "recommended ai automation for local businesses",
    "recommended ai automation for startups 2026",
    "recommended ai automation in 2026",
    "recommended ai automation for remote teams",
    "affordable ai automation for startups",
    "affordable ai automation for small business",
    "affordable ai automation for saas",
    "affordable ai automation for agencies",
    "affordable ai automation for ecommerce",
    "affordable ai automation for sales teams",
    "affordable ai automation for b2b",
    "affordable ai automation for enterprises",
    "affordable ai automation for customer support",
    "affordable ai automation for marketing teams",
    "affordable ai automation for real estate",
    "affordable ai automation for recruiters",
    "affordable ai automation for hr teams",
    "affordable ai automation for healthcare",
    "affordable ai automation for finance teams",
    "affordable ai automation for travel companies",
    "affordable ai automation for developers",
    "affordable ai automation for local businesses",
    "affordable ai automation for startups 2026",
    "affordable ai automation in 2026",
    "affordable ai automation for remote teams",
    "enterprise ai automation for startups",
    "enterprise ai automation for small business",
    "enterprise ai automation for saas",
    "enterprise ai automation for agencies",
    "enterprise ai automation for ecommerce",
    "enterprise ai automation for sales teams",
    "enterprise ai automation for b2b",
    "enterprise ai automation for enterprises",
    "enterprise ai automation for customer support",
    "enterprise ai automation for marketing teams",
    "enterprise ai automation for real estate",
    "enterprise ai automation for recruiters",
    "enterprise ai automation for hr teams",
    "enterprise ai automation for healthcare",
    "enterprise ai automation for finance teams",
    "enterprise ai automation for travel companies",
    "enterprise ai automation for developers",
    "enterprise ai automation for local businesses",
    "enterprise ai automation for startups 2026",
    "enterprise ai automation in 2026",
    "enterprise ai automation for remote teams",
    "small business ai automation for startups",
    "small business ai automation for small business",
    "small business ai automation for saas",
    "small business ai automation for agencies",
    "small business ai automation for ecommerce",
    "small business ai automation for sales teams",
    "small business ai automation for b2b",
    "small business ai automation for enterprises",
    "small business ai automation for customer support",
    "small business ai automation for marketing teams",
    "small business ai automation for real estate",
    "small business ai automation for recruiters",
    "small business ai automation for hr teams",
    "small business ai automation for healthcare",
    "small business ai automation for finance teams",
    "small business ai automation for travel companies",
    "small business ai automation for developers",
    "small business ai automation for local businesses",
    "small business ai automation for startups 2026",
    "small business ai automation in 2026",
    "small business ai automation for remote teams",
    "startup ai automation for startups",
    "startup ai automation for small business",
    "startup ai automation for saas",
    "startup ai automation for agencies",
    "startup ai automation for ecommerce",
    "startup ai automation for sales teams",
    "startup ai automation for b2b",
    "startup ai automation for enterprises",
    "startup ai automation for customer support",
    "startup ai automation for marketing teams",
    "startup ai automation for real estate",
    "startup ai automation for recruiters",
    "startup ai automation for hr teams",
    "startup ai automation for healthcare",
    "startup ai automation for finance teams",
    "startup ai automation for travel companies",
    "startup ai automation for developers",
    "startup ai automation for local businesses",
    "startup ai automation for startups 2026",
    "startup ai automation in 2026",
    "startup ai automation for remote teams",
    "alternative ai automation for startups",
    "alternative ai automation for small business",
    "alternative ai automation for saas",
    "alternative ai automation for agencies",
    "alternative ai automation for ecommerce",
    "alternative ai automation for sales teams",
    "alternative ai automation for b2b",
    "alternative ai automation for enterprises",
    "alternative ai automation for customer support",
    "alternative ai automation for marketing teams",
    "alternative ai automation for real estate",
    "alternative ai automation for recruiters",
    "alternative ai automation for hr teams",
    "alternative ai automation for healthcare",
    "alternative ai automation for finance teams",
    "alternative ai automation for travel companies",
    "alternative ai automation for developers",
    "alternative ai automation for local businesses",
    "alternative ai automation for startups 2026",
    "alternative ai automation in 2026",
    "alternative ai automation for remote teams",
    "alternatives ai automation for startups",
    "alternatives ai automation for small business",
    "alternatives ai automation for saas",
    "alternatives ai automation for agencies",
    "alternatives ai automation for ecommerce",
    "alternatives ai automation for sales teams",
    "alternatives ai automation for b2b",
    "alternatives ai automation for enterprises",
    "alternatives ai automation for customer support",
    "alternatives ai automation for marketing teams",
    "alternatives ai automation for real estate",
    "alternatives ai automation for recruiters",
    "alternatives ai automation for hr teams",
    "alternatives ai automation for healthcare",
    "alternatives ai automation for finance teams",
    "alternatives ai automation for travel companies",
    "alternatives ai automation for developers",
    "alternatives ai automation for local businesses",
    "alternatives ai automation for startups 2026",
    "alternatives ai automation in 2026",
    "alternatives ai automation for remote teams",
    "comparison ai automation for startups",
    "comparison ai automation for small business",
    "comparison ai automation for saas",
    "comparison ai automation for agencies",
    "comparison ai automation for ecommerce",
    "comparison ai automation for sales teams",
    "comparison ai automation for b2b",
    "comparison ai automation for enterprises",
    "comparison ai automation for customer support",
    "comparison ai automation for marketing teams",
    "comparison ai automation for real estate",
    "comparison ai automation for recruiters",
    "comparison ai automation for hr teams",
    "comparison ai automation for healthcare",
    "comparison ai automation for finance teams",
    "comparison ai automation for travel companies",
    "comparison ai automation for developers",
    "comparison ai automation for local businesses",
    "comparison ai automation for startups 2026",
    "comparison ai automation in 2026",
    "comparison ai automation for remote teams",
    "vs ai automation for startups",
    "vs ai automation for small business",
    "vs ai automation for saas",
    "vs ai automation for agencies",
    "vs ai automation for ecommerce",
    "vs ai automation for sales teams",
    "vs ai automation for b2b",
    "vs ai automation for enterprises",
    "vs ai automation for customer support",
    "vs ai automation for marketing teams",
    "vs ai automation for real estate",
    "vs ai automation for recruiters",
    "vs ai automation for hr teams",
    "vs ai automation for healthcare",
    "vs ai automation for finance teams",
    "vs ai automation for travel companies",
    "vs ai automation for developers",
    "vs ai automation for local businesses",
    "vs ai automation for startups 2026",
    "vs ai automation in 2026",
    "vs ai automation for remote teams",
    "pricing ai automation for startups",
    "pricing ai automation for small business",
    "pricing ai automation for saas",
    "pricing ai automation for agencies",
    "pricing ai automation for ecommerce",
    "pricing ai automation for sales teams",
    "pricing ai automation for b2b",
    "pricing ai automation for enterprises",
    "pricing ai automation for customer support",
    "pricing ai automation for marketing teams",
    "pricing ai automation for real estate",
    "pricing ai automation for recruiters",
    "pricing ai automation for hr teams",
    "pricing ai automation for healthcare",
    "pricing ai automation for finance teams",
    "pricing ai automation for travel companies",
    "pricing ai automation for developers",
    "pricing ai automation for local businesses",
    "pricing ai automation for startups 2026",
    "pricing ai automation in 2026",
    "pricing ai automation for remote teams",
    "review ai automation for startups",
    "review ai automation for small business",
    "review ai automation for saas",
    "review ai automation for agencies",
    "review ai automation for ecommerce",
    "review ai automation for sales teams",
    "review ai automation for b2b",
    "review ai automation for enterprises",
    "review ai automation for customer support",
    "review ai automation for marketing teams",
    "review ai automation for real estate",
    "review ai automation for recruiters",
    "review ai automation for hr teams",
    "review ai automation for healthcare",
    "review ai automation for finance teams",
    "review ai automation for travel companies",
    "review ai automation for developers",
    "review ai automation for local businesses",
    "review ai automation for startups 2026",
    "review ai automation in 2026",
    "review ai automation for remote teams",
    "software ai automation for startups",
    "software ai automation for small business",
    "software ai automation for saas",
    "software ai automation for agencies",
    "software ai automation for ecommerce",
    "software ai automation for sales teams",
    "software ai automation for b2b",
    "software ai automation for enterprises",
    "software ai automation for customer support",
    "software ai automation for marketing teams",
    "software ai automation for real estate",
    "software ai automation for recruiters",
    "software ai automation for hr teams",
    "software ai automation for healthcare",
    "software ai automation for finance teams",
    "software ai automation for travel companies",
    "software ai automation for developers",
    "software ai automation for local businesses",
    "software ai automation for startups 2026",
    "software ai automation in 2026",
    "software ai automation for remote teams",
    "platform ai automation for startups",
    "platform ai automation for small business",
    "platform ai automation for saas",
    "platform ai automation for agencies",
    "platform ai automation for ecommerce",
    "platform ai automation for sales teams",
    "platform ai automation for b2b",
    "platform ai automation for enterprises",
    "platform ai automation for customer support",
    "platform ai automation for marketing teams",
    "platform ai automation for real estate",
    "platform ai automation for recruiters",
    "platform ai automation for hr teams",
    "platform ai automation for healthcare",
    "platform ai automation for finance teams",
    "platform ai automation for travel companies",
    "platform ai automation for developers",
    "platform ai automation for local businesses",
    "platform ai automation for startups 2026",
    "platform ai automation in 2026",
    "platform ai automation for remote teams",
    "solution ai automation for startups",
    "solution ai automation for small business",
    "solution ai automation for saas",
    "solution ai automation for agencies",
    "solution ai automation for ecommerce",
    "solution ai automation for sales teams",
    "solution ai automation for b2b",
    "solution ai automation for enterprises",
    "solution ai automation for customer support",
    "solution ai automation for marketing teams",
    "solution ai automation for real estate",
    "solution ai automation for recruiters",
    "solution ai automation for hr teams",
    "solution ai automation for healthcare",
    "solution ai automation for finance teams",
    "solution ai automation for travel companies",
    "solution ai automation for developers",
    "solution ai automation for local businesses",
    "solution ai automation for startups 2026",
    "solution ai automation in 2026",
    "solution ai automation for remote teams",
    "tool ai automation for startups",
    "tool ai automation for small business",
    "tool ai automation for saas",
    "tool ai automation for agencies",
    "tool ai automation for ecommerce",
    "tool ai automation for sales teams",
    "tool ai automation for b2b",
    "tool ai automation for enterprises",
    "tool ai automation for customer support",
    "tool ai automation for marketing teams",
    "tool ai automation for real estate",
    "tool ai automation for recruiters",
    "tool ai automation for hr teams",
    "tool ai automation for healthcare",
    "tool ai automation for finance teams",
    "tool ai automation for travel companies",
    "tool ai automation for developers",
    "tool ai automation for local businesses",
    "tool ai automation for startups 2026",
    "tool ai automation in 2026",
    "tool ai automation for remote teams",
    "tools ai automation for startups",
    "tools ai automation for small business",
    "tools ai automation for saas",
    "tools ai automation for agencies",
    "tools ai automation for ecommerce",
    "tools ai automation for sales teams",
    "tools ai automation for b2b",
    "tools ai automation for enterprises",
    "tools ai automation for customer support",
    "tools ai automation for marketing teams",
    "tools ai automation for real estate",
    "tools ai automation for recruiters",
    "tools ai automation for hr teams",
    "tools ai automation for healthcare",
    "tools ai automation for finance teams",
    "tools ai automation for travel companies",
    "tools ai automation for developers",
    "tools ai automation for local businesses",
    "tools ai automation for startups 2026",
    "tools ai automation in 2026",
    "tools ai automation for remote teams",
    "provider ai automation for startups",
    "provider ai automation for small business",
    "provider ai automation for saas",
    "provider ai automation for agencies",
    "provider ai automation for ecommerce",
    "provider ai automation for sales teams",
    "provider ai automation for b2b",
    "provider ai automation for enterprises",
    "provider ai automation for customer support",
    "provider ai automation for marketing teams",
    "provider ai automation for real estate",
    "provider ai automation for recruiters",
    "provider ai automation for hr teams",
    "provider ai automation for healthcare",
    "provider ai automation for finance teams",
    "provider ai automation for travel companies",
    "provider ai automation for developers",
    "provider ai automation for local businesses",
    "provider ai automation for startups 2026",
    "provider ai automation in 2026",
    "provider ai automation for remote teams",
    "service ai automation for startups",
    "service ai automation for small business",
    "service ai automation for saas",
    "service ai automation for agencies",
    "service ai automation for ecommerce",
    "service ai automation for sales teams",
    "service ai automation for b2b",
    "service ai automation for enterprises",
    "service ai automation for customer support",
    "service ai automation for marketing teams",
    "service ai automation for real estate",
    "service ai automation for recruiters",
    "service ai automation for hr teams",
    "service ai automation for healthcare",
    "service ai automation for finance teams",
    "service ai automation for travel companies",
    "service ai automation for developers",
    "service ai automation for local businesses",
    "service ai automation for startups 2026",
    "service ai automation in 2026",
    "service ai automation for remote teams",
    "automation ai automation for startups",
    "automation ai automation for small business",
    "automation ai automation for saas",
    "automation ai automation for agencies",
    "automation ai automation for ecommerce",
    "automation ai automation for sales teams",
    "automation ai automation for b2b",
    "automation ai automation for enterprises",
    "automation ai automation for customer support",
    "automation ai automation for marketing teams",
    "automation ai automation for real estate",
    "automation ai automation for recruiters",
    "automation ai automation for hr teams",
    "automation ai automation for healthcare",
    "automation ai automation for finance teams",
    "automation ai automation for travel companies",
    "automation ai automation for developers",
    "automation ai automation for local businesses",
    "automation ai automation for startups 2026",
    "automation ai automation in 2026",
    "automation ai automation for remote teams",
    "best ai automation",
    "how to use ai automation",
    "how to choose ai automation",
    "looking for ai automation",
    "ai automation recommendations",
    "ai automation alternatives",
    "ai automation comparison",
    "ai automation pricing",
    "ai automation review",
    "ai automation for lead generation",
    "ai automation for customer support",
    "ai automation for sales",
    "ai automation for marketing",
    "ai automation for business automation",
    "ai automation for appointment booking",
    "ai automation for outbound outreach",
    "ai automation for ecommerce",
    "ai automation for workflow automation",
    "ai automation for research",
    "ai automation for business",
    "need ai automation",
    "recommend a ai automation",
    "what is the best ai automation",
    "which ai automation should i use",
    "best ai sales agent for startups",
    "best ai sales agent for small business",
    "best ai sales agent for saas",
    "best ai sales agent for agencies",
    "best ai sales agent for ecommerce",
    "best ai sales agent for sales teams",
    "best ai sales agent for b2b",
    "best ai sales agent for enterprises",
    "best ai sales agent for customer support",
    "best ai sales agent for marketing teams",
    "best ai sales agent for real estate",
    "best ai sales agent for recruiters",
    "best ai sales agent for hr teams",
    "best ai sales agent for healthcare",
    "best ai sales agent for finance teams",
    "best ai sales agent for travel companies",
    "best ai sales agent for developers",
    "best ai sales agent for local businesses",
    "best ai sales agent for startups 2026",
    "best ai sales agent in 2026",
    "best ai sales agent for remote teams",
    "top ai sales agent for startups",
    "top ai sales agent for small business",
    "top ai sales agent for saas",
    "top ai sales agent for agencies",
    "top ai sales agent for ecommerce",
    "top ai sales agent for sales teams",
    "top ai sales agent for b2b",
    "top ai sales agent for enterprises",
    "top ai sales agent for customer support",
    "top ai sales agent for marketing teams",
    "top ai sales agent for real estate",
    "top ai sales agent for recruiters",
    "top ai sales agent for hr teams",
    "top ai sales agent for healthcare",
    "top ai sales agent for finance teams",
    "top ai sales agent for travel companies",
    "top ai sales agent for developers",
    "top ai sales agent for local businesses",
    "top ai sales agent for startups 2026",
    "top ai sales agent in 2026",
    "top ai sales agent for remote teams",
    "recommended ai sales agent for startups",
    "recommended ai sales agent for small business",
    "recommended ai sales agent for saas",
    "recommended ai sales agent for agencies",
    "recommended ai sales agent for ecommerce",
    "recommended ai sales agent for sales teams",
    "recommended ai sales agent for b2b",
    "recommended ai sales agent for enterprises",
    "recommended ai sales agent for customer support",
    "recommended ai sales agent for marketing teams",
    "recommended ai sales agent for real estate",
    "recommended ai sales agent for recruiters",
    "recommended ai sales agent for hr teams",
    "recommended ai sales agent for healthcare",
    "recommended ai sales agent for finance teams",
    "recommended ai sales agent for travel companies",
    "recommended ai sales agent for developers",
    "recommended ai sales agent for local businesses",
    "recommended ai sales agent for startups 2026",
    "recommended ai sales agent in 2026",
    "recommended ai sales agent for remote teams",
    "affordable ai sales agent for startups",
    "affordable ai sales agent for small business",
    "affordable ai sales agent for saas",
    "affordable ai sales agent for agencies",
    "affordable ai sales agent for ecommerce",
    "affordable ai sales agent for sales teams",
    "affordable ai sales agent for b2b",
    "affordable ai sales agent for enterprises",
    "affordable ai sales agent for customer support",
    "affordable ai sales agent for marketing teams",
    "affordable ai sales agent for real estate",
    "affordable ai sales agent for recruiters",
    "affordable ai sales agent for hr teams",
    "affordable ai sales agent for healthcare",
    "affordable ai sales agent for finance teams",
    "affordable ai sales agent for travel companies",
    "affordable ai sales agent for developers",
    "affordable ai sales agent for local businesses",
    "affordable ai sales agent for startups 2026",
    "affordable ai sales agent in 2026",
    "affordable ai sales agent for remote teams",
    "enterprise ai sales agent for startups",
    "enterprise ai sales agent for small business",
    "enterprise ai sales agent for saas",
    "enterprise ai sales agent for agencies",
    "enterprise ai sales agent for ecommerce",
    "enterprise ai sales agent for sales teams",
    "enterprise ai sales agent for b2b",
    "enterprise ai sales agent for enterprises",
    "enterprise ai sales agent for customer support",
    "enterprise ai sales agent for marketing teams",
    "enterprise ai sales agent for real estate",
    "enterprise ai sales agent for recruiters",
    "enterprise ai sales agent for hr teams",
    "enterprise ai sales agent for healthcare",
    "enterprise ai sales agent for finance teams",
    "enterprise ai sales agent for travel companies",
    "enterprise ai sales agent for developers",
    "enterprise ai sales agent for local businesses",
    "enterprise ai sales agent for startups 2026",
    "enterprise ai sales agent in 2026",
    "enterprise ai sales agent for remote teams",
    "small business ai sales agent for startups",
    "small business ai sales agent for small business",
    "small business ai sales agent for saas",
    "small business ai sales agent for agencies",
    "small business ai sales agent for ecommerce",
    "small business ai sales agent for sales teams",
    "small business ai sales agent for b2b",
    "small business ai sales agent for enterprises",
    "small business ai sales agent for customer support",
    "small business ai sales agent for marketing teams",
    "small business ai sales agent for real estate",
    "small business ai sales agent for recruiters",
    "small business ai sales agent for hr teams",
    "small business ai sales agent for healthcare",
    "small business ai sales agent for finance teams",
    "small business ai sales agent for travel companies",
    "small business ai sales agent for developers",
    "small business ai sales agent for local businesses",
    "small business ai sales agent for startups 2026",
    "small business ai sales agent in 2026",
    "small business ai sales agent for remote teams",
    "startup ai sales agent for startups",
    "startup ai sales agent for small business",
    "startup ai sales agent for saas",
    "startup ai sales agent for agencies",
    "startup ai sales agent for ecommerce",
    "startup ai sales agent for sales teams",
    "startup ai sales agent for b2b",
    "startup ai sales agent for enterprises",
    "startup ai sales agent for customer support",
    "startup ai sales agent for marketing teams",
    "startup ai sales agent for real estate",
    "startup ai sales agent for recruiters",
    "startup ai sales agent for hr teams",
    "startup ai sales agent for healthcare",
    "startup ai sales agent for finance teams",
    "startup ai sales agent for travel companies",
    "startup ai sales agent for developers",
    "startup ai sales agent for local businesses",
    "startup ai sales agent for startups 2026",
    "startup ai sales agent in 2026",
    "startup ai sales agent for remote teams",
    "alternative ai sales agent for startups",
    "alternative ai sales agent for small business",
    "alternative ai sales agent for saas",
    "alternative ai sales agent for agencies",
    "alternative ai sales agent for ecommerce",
    "alternative ai sales agent for sales teams",
    "alternative ai sales agent for b2b",
    "alternative ai sales agent for enterprises",
    "alternative ai sales agent for customer support",
    "alternative ai sales agent for marketing teams",
    "alternative ai sales agent for real estate",
    "alternative ai sales agent for recruiters",
    "alternative ai sales agent for hr teams",
    "alternative ai sales agent for healthcare",
    "alternative ai sales agent for finance teams",
    "alternative ai sales agent for travel companies",
    "alternative ai sales agent for developers",
    "alternative ai sales agent for local businesses",
    "alternative ai sales agent for startups 2026",
    "alternative ai sales agent in 2026",
    "alternative ai sales agent for remote teams",
    "alternatives ai sales agent for startups",
    "alternatives ai sales agent for small business",
    "alternatives ai sales agent for saas",
    "alternatives ai sales agent for agencies",
    "alternatives ai sales agent for ecommerce",
    "alternatives ai sales agent for sales teams",
    "alternatives ai sales agent for b2b",
    "alternatives ai sales agent for enterprises",
    "alternatives ai sales agent for customer support",
    "alternatives ai sales agent for marketing teams",
    "alternatives ai sales agent for real estate",
    "alternatives ai sales agent for recruiters",
    "alternatives ai sales agent for hr teams",
    "alternatives ai sales agent for healthcare",
    "alternatives ai sales agent for finance teams",
    "alternatives ai sales agent for travel companies",
    "alternatives ai sales agent for developers",
    "alternatives ai sales agent for local businesses",
    "alternatives ai sales agent for startups 2026",
    "alternatives ai sales agent in 2026",
    "alternatives ai sales agent for remote teams",
    "comparison ai sales agent for startups",
    "comparison ai sales agent for small business",
    "comparison ai sales agent for saas",
    "comparison ai sales agent for agencies",
    "comparison ai sales agent for ecommerce",
    "comparison ai sales agent for sales teams",
    "comparison ai sales agent for b2b",
    "comparison ai sales agent for enterprises",
    "comparison ai sales agent for customer support",
    "comparison ai sales agent for marketing teams",
    "comparison ai sales agent for real estate",
    "comparison ai sales agent for recruiters",
    "comparison ai sales agent for hr teams",
    "comparison ai sales agent for healthcare",
    "comparison ai sales agent for finance teams",
    "comparison ai sales agent for travel companies",
    "comparison ai sales agent for developers",
    "comparison ai sales agent for local businesses",
    "comparison ai sales agent for startups 2026",
    "comparison ai sales agent in 2026",
    "comparison ai sales agent for remote teams",
    "vs ai sales agent for startups",
    "vs ai sales agent for small business",
    "vs ai sales agent for saas",
    "vs ai sales agent for agencies",
    "vs ai sales agent for ecommerce",
    "vs ai sales agent for sales teams",
    "vs ai sales agent for b2b",
    "vs ai sales agent for enterprises",
    "vs ai sales agent for customer support",
    "vs ai sales agent for marketing teams",
    "vs ai sales agent for real estate",
    "vs ai sales agent for recruiters",
    "vs ai sales agent for hr teams",
    "vs ai sales agent for healthcare",
    "vs ai sales agent for finance teams",
    "vs ai sales agent for travel companies",
    "vs ai sales agent for developers",
    "vs ai sales agent for local businesses",
    "vs ai sales agent for startups 2026",
    "vs ai sales agent in 2026",
    "vs ai sales agent for remote teams",
    "pricing ai sales agent for startups",
    "pricing ai sales agent for small business",
    "pricing ai sales agent for saas",
    "pricing ai sales agent for agencies",
    "pricing ai sales agent for ecommerce",
    "pricing ai sales agent for sales teams",
    "pricing ai sales agent for b2b",
    "pricing ai sales agent for enterprises",
    "pricing ai sales agent for customer support",
    "pricing ai sales agent for marketing teams",
    "pricing ai sales agent for real estate",
    "pricing ai sales agent for recruiters",
    "pricing ai sales agent for hr teams",
    "pricing ai sales agent for healthcare",
    "pricing ai sales agent for finance teams",
    "pricing ai sales agent for travel companies",
    "pricing ai sales agent for developers",
    "pricing ai sales agent for local businesses",
    "pricing ai sales agent for startups 2026",
    "pricing ai sales agent in 2026",
    "pricing ai sales agent for remote teams",
    "review ai sales agent for startups",
    "review ai sales agent for small business",
    "review ai sales agent for saas",
    "review ai sales agent for agencies",
    "review ai sales agent for ecommerce",
    "review ai sales agent for sales teams",
    "review ai sales agent for b2b",
    "review ai sales agent for enterprises",
    "review ai sales agent for customer support",
    "review ai sales agent for marketing teams",
    "review ai sales agent for real estate",
    "review ai sales agent for recruiters",
    "review ai sales agent for hr teams",
    "review ai sales agent for healthcare",
    "review ai sales agent for finance teams",
    "review ai sales agent for travel companies",
    "review ai sales agent for developers",
    "review ai sales agent for local businesses",
    "review ai sales agent for startups 2026",
    "review ai sales agent in 2026",
    "review ai sales agent for remote teams",
    "software ai sales agent for startups",
    "software ai sales agent for small business",
    "software ai sales agent for saas",
    "software ai sales agent for agencies",
    "software ai sales agent for ecommerce",
    "software ai sales agent for sales teams",
    "software ai sales agent for b2b",
    "software ai sales agent for enterprises",
    "software ai sales agent for customer support",
    "software ai sales agent for marketing teams",
    "software ai sales agent for real estate",
    "software ai sales agent for recruiters",
    "software ai sales agent for hr teams",
    "software ai sales agent for healthcare",
    "software ai sales agent for finance teams",
    "software ai sales agent for travel companies",
    "software ai sales agent for developers",
    "software ai sales agent for local businesses",
    "software ai sales agent for startups 2026",
    "software ai sales agent in 2026",
    "software ai sales agent for remote teams",
    "platform ai sales agent for startups",
    "platform ai sales agent for small business",
    "platform ai sales agent for saas",
    "platform ai sales agent for agencies",
    "platform ai sales agent for ecommerce",
    "platform ai sales agent for sales teams",
    "platform ai sales agent for b2b",
    "platform ai sales agent for enterprises",
    "platform ai sales agent for customer support",
    "platform ai sales agent for marketing teams",
    "platform ai sales agent for real estate",
    "platform ai sales agent for recruiters",
    "platform ai sales agent for hr teams",
    "platform ai sales agent for healthcare",
    "platform ai sales agent for finance teams",
    "platform ai sales agent for travel companies",
    "platform ai sales agent for developers",
    "platform ai sales agent for local businesses",
    "platform ai sales agent for startups 2026",
    "platform ai sales agent in 2026",
    "platform ai sales agent for remote teams",
    "solution ai sales agent for startups",
    "solution ai sales agent for small business",
    "solution ai sales agent for saas",
    "solution ai sales agent for agencies",
    "solution ai sales agent for ecommerce",
    "solution ai sales agent for sales teams",
    "solution ai sales agent for b2b",
    "solution ai sales agent for enterprises",
    "solution ai sales agent for customer support",
    "solution ai sales agent for marketing teams",
    "solution ai sales agent for real estate",
    "solution ai sales agent for recruiters",
    "solution ai sales agent for hr teams",
    "solution ai sales agent for healthcare",
    "solution ai sales agent for finance teams",
    "solution ai sales agent for travel companies",
    "solution ai sales agent for developers",
    "solution ai sales agent for local businesses",
    "solution ai sales agent for startups 2026",
    "solution ai sales agent in 2026",
    "solution ai sales agent for remote teams",
    "tool ai sales agent for startups",
    "tool ai sales agent for small business",
    "tool ai sales agent for saas",
    "tool ai sales agent for agencies",
    "tool ai sales agent for ecommerce",
    "tool ai sales agent for sales teams",
    "tool ai sales agent for b2b",
    "tool ai sales agent for enterprises",
    "tool ai sales agent for customer support",
    "tool ai sales agent for marketing teams",
    "tool ai sales agent for real estate",
    "tool ai sales agent for recruiters",
    "tool ai sales agent for hr teams",
    "tool ai sales agent for healthcare",
    "tool ai sales agent for finance teams",
    "tool ai sales agent for travel companies",
    "tool ai sales agent for developers",
    "tool ai sales agent for local businesses",
    "tool ai sales agent for startups 2026",
    "tool ai sales agent in 2026",
    "tool ai sales agent for remote teams",
    "tools ai sales agent for startups",
    "tools ai sales agent for small business",
    "tools ai sales agent for saas",
    "tools ai sales agent for agencies",
    "tools ai sales agent for ecommerce",
    "tools ai sales agent for sales teams",
    "tools ai sales agent for b2b",
    "tools ai sales agent for enterprises",
    "tools ai sales agent for customer support",
    "tools ai sales agent for marketing teams",
    "tools ai sales agent for real estate",
    "tools ai sales agent for recruiters",
    "tools ai sales agent for hr teams",
    "tools ai sales agent for healthcare",
    "tools ai sales agent for finance teams",
    "tools ai sales agent for travel companies",
    "tools ai sales agent for developers",
    "tools ai sales agent for local businesses",
    "tools ai sales agent for startups 2026",
    "tools ai sales agent in 2026",
    "tools ai sales agent for remote teams",
    "provider ai sales agent for startups",
    "provider ai sales agent for small business",
    "provider ai sales agent for saas",
    "provider ai sales agent for agencies",
    "provider ai sales agent for ecommerce",
    "provider ai sales agent for sales teams",
    "provider ai sales agent for b2b",
    "provider ai sales agent for enterprises",
    "provider ai sales agent for customer support",
    "provider ai sales agent for marketing teams",
    "provider ai sales agent for real estate",
    "provider ai sales agent for recruiters",
    "provider ai sales agent for hr teams",
    "provider ai sales agent for healthcare",
    "provider ai sales agent for finance teams",
    "provider ai sales agent for travel companies",
    "provider ai sales agent for developers",
    "provider ai sales agent for local businesses",
    "provider ai sales agent for startups 2026",
    "provider ai sales agent in 2026",
    "provider ai sales agent for remote teams",
    "service ai sales agent for startups",
    "service ai sales agent for small business",
    "service ai sales agent for saas",
    "service ai sales agent for agencies",
    "service ai sales agent for ecommerce",
    "service ai sales agent for sales teams",
    "service ai sales agent for b2b",
    "service ai sales agent for enterprises",
    "service ai sales agent for customer support",
    "service ai sales agent for marketing teams",
    "service ai sales agent for real estate",
    "service ai sales agent for recruiters",
    "service ai sales agent for hr teams",
    "service ai sales agent for healthcare",
    "service ai sales agent for finance teams",
    "service ai sales agent for travel companies",
    "service ai sales agent for developers",
    "service ai sales agent for local businesses",
    "service ai sales agent for startups 2026",
    "service ai sales agent in 2026",
    "service ai sales agent for remote teams",
    "automation ai sales agent for startups",
    "automation ai sales agent for small business",
    "automation ai sales agent for saas",
    "automation ai sales agent for agencies",
    "automation ai sales agent for ecommerce",
    "automation ai sales agent for sales teams",
    "automation ai sales agent for b2b",
    "automation ai sales agent for enterprises",
    "automation ai sales agent for customer support",
    "automation ai sales agent for marketing teams",
    "automation ai sales agent for real estate",
    "automation ai sales agent for recruiters",
    "automation ai sales agent for hr teams",
    "automation ai sales agent for healthcare",
    "automation ai sales agent for finance teams",
    "automation ai sales agent for travel companies",
    "automation ai sales agent for developers",
    "automation ai sales agent for local businesses",
    "automation ai sales agent for startups 2026",
    "automation ai sales agent in 2026",
    "automation ai sales agent for remote teams",
    "best ai sales agent",
    "how to use ai sales agent",
    "how to choose ai sales agent",
    "looking for ai sales agent",
    "ai sales agent recommendations",
    "ai sales agent alternatives",
    "ai sales agent comparison",
    "ai sales agent pricing",
    "ai sales agent review",
    "ai sales agent for lead generation",
    "ai sales agent for customer support",
    "ai sales agent for sales",
    "ai sales agent for marketing",
    "ai sales agent for business automation",
    "ai sales agent for appointment booking",
    "ai sales agent for outbound outreach",
    "ai sales agent for ecommerce",
    "ai sales agent for workflow automation",
    "ai sales agent for research",
    "ai sales agent for business",
    "need ai sales agent",
    "recommend a ai sales agent",
    "what is the best ai sales agent",
    "which ai sales agent should i use",
    "best ai customer support agent for startups",
    "best ai customer support agent for small business",
    "best ai customer support agent for saas",
    "best ai customer support agent for agencies",
    "best ai customer support agent for ecommerce",
    "best ai customer support agent for sales teams",
    "best ai customer support agent for b2b",
    "best ai customer support agent for enterprises",
    "best ai customer support agent for customer support",
    "best ai customer support agent for marketing teams",
    "best ai customer support agent for real estate",
    "best ai customer support agent for recruiters",
    "best ai customer support agent for hr teams",
    "best ai customer support agent for healthcare",
    "best ai customer support agent for finance teams",
    "best ai customer support agent for travel companies",
    "best ai customer support agent for developers",
    "best ai customer support agent for local businesses",
    "best ai customer support agent for startups 2026",
    "best ai customer support agent in 2026",
    "best ai customer support agent for remote teams",
    "top ai customer support agent for startups",
    "top ai customer support agent for small business",
    "top ai customer support agent for saas",
    "top ai customer support agent for agencies",
    "top ai customer support agent for ecommerce",
    "top ai customer support agent for sales teams",
    "top ai customer support agent for b2b",
    "top ai customer support agent for enterprises",
    "top ai customer support agent for customer support",
    "top ai customer support agent for marketing teams",
    "top ai customer support agent for real estate",
    "top ai customer support agent for recruiters",
    "top ai customer support agent for hr teams",
    "top ai customer support agent for healthcare",
    "top ai customer support agent for finance teams",
    "top ai customer support agent for travel companies",
    "top ai customer support agent for developers",
    "top ai customer support agent for local businesses",
    "top ai customer support agent for startups 2026",
    "top ai customer support agent in 2026",
    "top ai customer support agent for remote teams",
    "recommended ai customer support agent for startups",
    "recommended ai customer support agent for small business",
    "recommended ai customer support agent for saas",
    "recommended ai customer support agent for agencies",
    "recommended ai customer support agent for ecommerce",
    "recommended ai customer support agent for sales teams",
    "recommended ai customer support agent for b2b",
    "recommended ai customer support agent for enterprises",
    "recommended ai customer support agent for customer support",
    "recommended ai customer support agent for marketing teams",
    "recommended ai customer support agent for real estate",
    "recommended ai customer support agent for recruiters",
    "recommended ai customer support agent for hr teams",
    "recommended ai customer support agent for healthcare",
    "recommended ai customer support agent for finance teams",
    "recommended ai customer support agent for travel companies",
    "recommended ai customer support agent for developers",
    "recommended ai customer support agent for local businesses",
    "recommended ai customer support agent for startups 2026",
    "recommended ai customer support agent in 2026",
    "recommended ai customer support agent for remote teams",
    "affordable ai customer support agent for startups",
    "affordable ai customer support agent for small business",
    "affordable ai customer support agent for saas",
    "affordable ai customer support agent for agencies",
    "affordable ai customer support agent for ecommerce",
    "affordable ai customer support agent for sales teams",
    "affordable ai customer support agent for b2b",
    "affordable ai customer support agent for enterprises",
    "affordable ai customer support agent for customer support",
    "affordable ai customer support agent for marketing teams",
    "affordable ai customer support agent for real estate",
    "affordable ai customer support agent for recruiters",
    "affordable ai customer support agent for hr teams",
    "affordable ai customer support agent for healthcare",
    "affordable ai customer support agent for finance teams",
    "affordable ai customer support agent for travel companies",
    "affordable ai customer support agent for developers",
    "affordable ai customer support agent for local businesses",
    "affordable ai customer support agent for startups 2026",
    "affordable ai customer support agent in 2026",
    "affordable ai customer support agent for remote teams",
    "enterprise ai customer support agent for startups",
    "enterprise ai customer support agent for small business",
    "enterprise ai customer support agent for saas",
    "enterprise ai customer support agent for agencies",
    "enterprise ai customer support agent for ecommerce",
    "enterprise ai customer support agent for sales teams",
    "enterprise ai customer support agent for b2b",
    "enterprise ai customer support agent for enterprises",
    "enterprise ai customer support agent for customer support",
    "enterprise ai customer support agent for marketing teams",
    "enterprise ai customer support agent for real estate",
    "enterprise ai customer support agent for recruiters",
    "enterprise ai customer support agent for hr teams",
    "enterprise ai customer support agent for healthcare",
    "enterprise ai customer support agent for finance teams",
    "enterprise ai customer support agent for travel companies",
    "enterprise ai customer support agent for developers",
    "enterprise ai customer support agent for local businesses",
    "enterprise ai customer support agent for startups 2026",
    "enterprise ai customer support agent in 2026",
    "enterprise ai customer support agent for remote teams",
    "small business ai customer support agent for startups",
    "small business ai customer support agent for small business",
    "small business ai customer support agent for saas",
    "small business ai customer support agent for agencies",
    "small business ai customer support agent for ecommerce",
    "small business ai customer support agent for sales teams",
    "small business ai customer support agent for b2b",
    "small business ai customer support agent for enterprises",
    "small business ai customer support agent for customer support",
    "small business ai customer support agent for marketing teams",
    "small business ai customer support agent for real estate",
    "small business ai customer support agent for recruiters",
    "small business ai customer support agent for hr teams",
    "small business ai customer support agent for healthcare",
    "small business ai customer support agent for finance teams",
    "small business ai customer support agent for travel companies",
    "small business ai customer support agent for developers",
    "small business ai customer support agent for local businesses",
    "small business ai customer support agent for startups 2026",
    "small business ai customer support agent in 2026",
    "small business ai customer support agent for remote teams",
    "startup ai customer support agent for startups",
    "startup ai customer support agent for small business",
    "startup ai customer support agent for saas",
    "startup ai customer support agent for agencies",
    "startup ai customer support agent for ecommerce",
    "startup ai customer support agent for sales teams",
    "startup ai customer support agent for b2b",
    "startup ai customer support agent for enterprises",
    "startup ai customer support agent for customer support",
    "startup ai customer support agent for marketing teams",
    "startup ai customer support agent for real estate",
    "startup ai customer support agent for recruiters",
    "startup ai customer support agent for hr teams",
    "startup ai customer support agent for healthcare",
    "startup ai customer support agent for finance teams",
    "startup ai customer support agent for travel companies",
    "startup ai customer support agent for developers",
    "startup ai customer support agent for local businesses",
    "startup ai customer support agent for startups 2026",
    "startup ai customer support agent in 2026",
    "startup ai customer support agent for remote teams",
    "alternative ai customer support agent for startups",
    "alternative ai customer support agent for small business",
    "alternative ai customer support agent for saas",
    "alternative ai customer support agent for agencies",
    "alternative ai customer support agent for ecommerce",
    "alternative ai customer support agent for sales teams",
    "alternative ai customer support agent for b2b",
    "alternative ai customer support agent for enterprises",
    "alternative ai customer support agent for customer support",
    "alternative ai customer support agent for marketing teams",
    "alternative ai customer support agent for real estate",
    "alternative ai customer support agent for recruiters",
    "alternative ai customer support agent for hr teams",
    "alternative ai customer support agent for healthcare",
    "alternative ai customer support agent for finance teams",
    "alternative ai customer support agent for travel companies",
    "alternative ai customer support agent for developers",
    "alternative ai customer support agent for local businesses",
    "alternative ai customer support agent for startups 2026",
    "alternative ai customer support agent in 2026",
    "alternative ai customer support agent for remote teams",
    "alternatives ai customer support agent for startups",
    "alternatives ai customer support agent for small business",
    "alternatives ai customer support agent for saas",
    "alternatives ai customer support agent for agencies",
    "alternatives ai customer support agent for ecommerce",
    "alternatives ai customer support agent for sales teams",
    "alternatives ai customer support agent for b2b",
    "alternatives ai customer support agent for enterprises",
    "alternatives ai customer support agent for customer support",
    "alternatives ai customer support agent for marketing teams",
    "alternatives ai customer support agent for real estate",
    "alternatives ai customer support agent for recruiters",
    "alternatives ai customer support agent for hr teams",
    "alternatives ai customer support agent for healthcare",
    "alternatives ai customer support agent for finance teams",
    "alternatives ai customer support agent for travel companies",
    "alternatives ai customer support agent for developers",
    "alternatives ai customer support agent for local businesses",
    "alternatives ai customer support agent for startups 2026",
    "alternatives ai customer support agent in 2026",
    "alternatives ai customer support agent for remote teams",
    "comparison ai customer support agent for startups",
    "comparison ai customer support agent for small business",
    "comparison ai customer support agent for saas",
    "comparison ai customer support agent for agencies",
    "comparison ai customer support agent for ecommerce",
    "comparison ai customer support agent for sales teams",
    "comparison ai customer support agent for b2b",
    "comparison ai customer support agent for enterprises",
    "comparison ai customer support agent for customer support",
    "comparison ai customer support agent for marketing teams",
    "comparison ai customer support agent for real estate",
    "comparison ai customer support agent for recruiters",
    "comparison ai customer support agent for hr teams",
    "comparison ai customer support agent for healthcare",
    "comparison ai customer support agent for finance teams",
    "comparison ai customer support agent for travel companies",
    "comparison ai customer support agent for developers",
    "comparison ai customer support agent for local businesses",
    "comparison ai customer support agent for startups 2026",
    "comparison ai customer support agent in 2026",
    "comparison ai customer support agent for remote teams",
    "vs ai customer support agent for startups",
    "vs ai customer support agent for small business",
    "vs ai customer support agent for saas",
    "vs ai customer support agent for agencies",
    "vs ai customer support agent for ecommerce",
    "vs ai customer support agent for sales teams",
    "vs ai customer support agent for b2b",
    "vs ai customer support agent for enterprises",
    "vs ai customer support agent for customer support",
    "vs ai customer support agent for marketing teams",
    "vs ai customer support agent for real estate",
    "vs ai customer support agent for recruiters",
    "vs ai customer support agent for hr teams",
    "vs ai customer support agent for healthcare",
    "vs ai customer support agent for finance teams",
    "vs ai customer support agent for travel companies",
    "vs ai customer support agent for developers",
    "vs ai customer support agent for local businesses",
    "vs ai customer support agent for startups 2026",
    "vs ai customer support agent in 2026",
    "vs ai customer support agent for remote teams",
    "pricing ai customer support agent for startups",
    "pricing ai customer support agent for small business",
    "pricing ai customer support agent for saas",
    "pricing ai customer support agent for agencies",
    "pricing ai customer support agent for ecommerce",
    "pricing ai customer support agent for sales teams",
    "pricing ai customer support agent for b2b",
    "pricing ai customer support agent for enterprises",
    "pricing ai customer support agent for customer support",
    "pricing ai customer support agent for marketing teams",
    "pricing ai customer support agent for real estate",
    "pricing ai customer support agent for recruiters",
    "pricing ai customer support agent for hr teams",
    "pricing ai customer support agent for healthcare",
    "pricing ai customer support agent for finance teams",
    "pricing ai customer support agent for travel companies",
    "pricing ai customer support agent for developers",
    "pricing ai customer support agent for local businesses",
    "pricing ai customer support agent for startups 2026",
    "pricing ai customer support agent in 2026",
    "pricing ai customer support agent for remote teams",
    "review ai customer support agent for startups",
    "review ai customer support agent for small business",
    "review ai customer support agent for saas",
    "review ai customer support agent for agencies",
    "review ai customer support agent for ecommerce",
    "review ai customer support agent for sales teams",
    "review ai customer support agent for b2b",
    "review ai customer support agent for enterprises",
    "review ai customer support agent for customer support",
    "review ai customer support agent for marketing teams",
    "review ai customer support agent for real estate",
    "review ai customer support agent for recruiters",
    "review ai customer support agent for hr teams",
    "review ai customer support agent for healthcare",
    "review ai customer support agent for finance teams",
    "review ai customer support agent for travel companies",
    "review ai customer support agent for developers",
    "review ai customer support agent for local businesses",
    "review ai customer support agent for startups 2026",
    "review ai customer support agent in 2026",
    "review ai customer support agent for remote teams",
    "software ai customer support agent for startups",
    "software ai customer support agent for small business",
    "software ai customer support agent for saas",
    "software ai customer support agent for agencies",
    "software ai customer support agent for ecommerce",
    "software ai customer support agent for sales teams",
    "software ai customer support agent for b2b",
    "software ai customer support agent for enterprises",
    "software ai customer support agent for customer support",
    "software ai customer support agent for marketing teams",
    "software ai customer support agent for real estate",
    "software ai customer support agent for recruiters",
    "software ai customer support agent for hr teams",
    "software ai customer support agent for healthcare",
    "software ai customer support agent for finance teams",
    "software ai customer support agent for travel companies",
    "software ai customer support agent for developers",
    "software ai customer support agent for local businesses",
    "software ai customer support agent for startups 2026",
    "software ai customer support agent in 2026",
    "software ai customer support agent for remote teams",
    "platform ai customer support agent for startups",
    "platform ai customer support agent for small business",
    "platform ai customer support agent for saas",
    "platform ai customer support agent for agencies",
    "platform ai customer support agent for ecommerce",
    "platform ai customer support agent for sales teams",
    "platform ai customer support agent for b2b",
    "platform ai customer support agent for enterprises",
    "platform ai customer support agent for customer support",
    "platform ai customer support agent for marketing teams",
    "platform ai customer support agent for real estate",
    "platform ai customer support agent for recruiters",
    "platform ai customer support agent for hr teams",
    "platform ai customer support agent for healthcare",
    "platform ai customer support agent for finance teams",
    "platform ai customer support agent for travel companies",
    "platform ai customer support agent for developers",
    "platform ai customer support agent for local businesses",
    "platform ai customer support agent for startups 2026",
    "platform ai customer support agent in 2026",
    "platform ai customer support agent for remote teams",
    "solution ai customer support agent for startups",
    "solution ai customer support agent for small business",
    "solution ai customer support agent for saas",
    "solution ai customer support agent for agencies",
    "solution ai customer support agent for ecommerce",
    "solution ai customer support agent for sales teams",
    "solution ai customer support agent for b2b",
    "solution ai customer support agent for enterprises",
    "solution ai customer support agent for customer support",
    "solution ai customer support agent for marketing teams",
    "solution ai customer support agent for real estate",
    "solution ai customer support agent for recruiters",
    "solution ai customer support agent for hr teams",
    "solution ai customer support agent for healthcare",
    "solution ai customer support agent for finance teams",
    "solution ai customer support agent for travel companies",
    "solution ai customer support agent for developers",
    "solution ai customer support agent for local businesses",
    "solution ai customer support agent for startups 2026",
    "solution ai customer support agent in 2026",
    "solution ai customer support agent for remote teams",
    "tool ai customer support agent for startups",
    "tool ai customer support agent for small business",
    "tool ai customer support agent for saas",
    "tool ai customer support agent for agencies",
    "tool ai customer support agent for ecommerce",
    "tool ai customer support agent for sales teams",
    "tool ai customer support agent for b2b",
    "tool ai customer support agent for enterprises",
    "tool ai customer support agent for customer support",
    "tool ai customer support agent for marketing teams",
    "tool ai customer support agent for real estate",
    "tool ai customer support agent for recruiters",
    "tool ai customer support agent for hr teams",
    "tool ai customer support agent for healthcare",
    "tool ai customer support agent for finance teams",
    "tool ai customer support agent for travel companies",
    "tool ai customer support agent for developers",
    "tool ai customer support agent for local businesses",
    "tool ai customer support agent for startups 2026",
    "tool ai customer support agent in 2026",
    "tool ai customer support agent for remote teams",
    "tools ai customer support agent for startups",
    "tools ai customer support agent for small business",
    "tools ai customer support agent for saas",
    "tools ai customer support agent for agencies",
    "tools ai customer support agent for ecommerce",
    "tools ai customer support agent for sales teams",
    "tools ai customer support agent for b2b",
    "tools ai customer support agent for enterprises",
    "tools ai customer support agent for customer support",
    "tools ai customer support agent for marketing teams",
    "tools ai customer support agent for real estate",
    "tools ai customer support agent for recruiters",
    "tools ai customer support agent for hr teams",
    "tools ai customer support agent for healthcare",
    "tools ai customer support agent for finance teams",
    "tools ai customer support agent for travel companies",
    "tools ai customer support agent for developers",
    "tools ai customer support agent for local businesses",
    "tools ai customer support agent for startups 2026",
    "tools ai customer support agent in 2026",
    "tools ai customer support agent for remote teams",
    "provider ai customer support agent for startups",
    "provider ai customer support agent for small business",
    "provider ai customer support agent for saas",
    "provider ai customer support agent for agencies",
    "provider ai customer support agent for ecommerce",
    "provider ai customer support agent for sales teams",
    "provider ai customer support agent for b2b",
    "provider ai customer support agent for enterprises",
    "provider ai customer support agent for customer support",
    "provider ai customer support agent for marketing teams",
    "provider ai customer support agent for real estate",
    "provider ai customer support agent for recruiters",
    "provider ai customer support agent for hr teams",
    "provider ai customer support agent for healthcare",
    "provider ai customer support agent for finance teams",
    "provider ai customer support agent for travel companies",
    "provider ai customer support agent for developers",
    "provider ai customer support agent for local businesses",
    "provider ai customer support agent for startups 2026",
    "provider ai customer support agent in 2026",
    "provider ai customer support agent for remote teams",
    "service ai customer support agent for startups",
    "service ai customer support agent for small business",
    "service ai customer support agent for saas",
    "service ai customer support agent for agencies",
    "service ai customer support agent for ecommerce",
    "service ai customer support agent for sales teams",
    "service ai customer support agent for b2b",
    "service ai customer support agent for enterprises",
    "service ai customer support agent for customer support",
    "service ai customer support agent for marketing teams",
    "service ai customer support agent for real estate",
    "service ai customer support agent for recruiters",
    "service ai customer support agent for hr teams",
    "service ai customer support agent for healthcare",
    "service ai customer support agent for finance teams",
    "service ai customer support agent for travel companies",
    "service ai customer support agent for developers",
    "service ai customer support agent for local businesses",
    "service ai customer support agent for startups 2026",
    "service ai customer support agent in 2026",
    "service ai customer support agent for remote teams",
    "automation ai customer support agent for startups",
    "automation ai customer support agent for small business",
    "automation ai customer support agent for saas",
    "automation ai customer support agent for agencies",
    "automation ai customer support agent for ecommerce",
    "automation ai customer support agent for sales teams",
    "automation ai customer support agent for b2b",
    "automation ai customer support agent for enterprises",
    "automation ai customer support agent for customer support",
    "automation ai customer support agent for marketing teams",
    "automation ai customer support agent for real estate",
    "automation ai customer support agent for recruiters",
    "automation ai customer support agent for hr teams",
    "automation ai customer support agent for healthcare",
    "automation ai customer support agent for finance teams",
    "automation ai customer support agent for travel companies",
    "automation ai customer support agent for developers",
    "automation ai customer support agent for local businesses",
    "automation ai customer support agent for startups 2026",
    "automation ai customer support agent in 2026",
    "automation ai customer support agent for remote teams",
    "best ai customer support agent",
    "how to use ai customer support agent",
    "how to choose ai customer support agent",
    "looking for ai customer support agent",
    "ai customer support agent recommendations",
    "ai customer support agent alternatives",
    "ai customer support agent comparison",
    "ai customer support agent pricing",
    "ai customer support agent review",
    "ai customer support agent for lead generation",
    "ai customer support agent for customer support",
    "ai customer support agent for sales",
    "ai customer support agent for marketing",
    "ai customer support agent for business automation",
    "ai customer support agent for appointment booking",
    "ai customer support agent for outbound outreach",
    "ai customer support agent for ecommerce",
    "ai customer support agent for workflow automation",
    "ai customer support agent for research",
    "ai customer support agent for business",
    "need ai customer support agent",
    "recommend a ai customer support agent",
    "what is the best ai customer support agent",
    "which ai customer support agent should i use",
    "best ai voice agent for startups",
    "best ai voice agent for small business",
    "best ai voice agent for saas",
    "best ai voice agent for agencies",
    "best ai voice agent for ecommerce",
    "best ai voice agent for sales teams",
    "best ai voice agent for b2b",
    "best ai voice agent for enterprises",
    "best ai voice agent for customer support",
    "best ai voice agent for marketing teams",
    "best ai voice agent for real estate",
    "best ai voice agent for recruiters",
    "best ai voice agent for hr teams",
    "best ai voice agent for healthcare",
    "best ai voice agent for finance teams",
    "best ai voice agent for travel companies",
    "best ai voice agent for developers",
    "best ai voice agent for local businesses",
    "best ai voice agent for startups 2026",
    "best ai voice agent in 2026",
    "best ai voice agent for remote teams",
    "top ai voice agent for startups",
    "top ai voice agent for small business",
    "top ai voice agent for saas",
    "top ai voice agent for agencies",
    "top ai voice agent for ecommerce",
    "top ai voice agent for sales teams",
    "top ai voice agent for b2b",
    "top ai voice agent for enterprises",
    "top ai voice agent for customer support",
    "top ai voice agent for marketing teams",
    "top ai voice agent for real estate",
    "top ai voice agent for recruiters",
    "top ai voice agent for hr teams",
    "top ai voice agent for healthcare",
    "top ai voice agent for finance teams",
    "top ai voice agent for travel companies",
    "top ai voice agent for developers",
    "top ai voice agent for local businesses",
    "top ai voice agent for startups 2026",
    "top ai voice agent in 2026",
    "top ai voice agent for remote teams",
    "recommended ai voice agent for startups",
    "recommended ai voice agent for small business",
    "recommended ai voice agent for saas",
    "recommended ai voice agent for agencies",
    "recommended ai voice agent for ecommerce",
    "recommended ai voice agent for sales teams",
    "recommended ai voice agent for b2b",
    "recommended ai voice agent for enterprises",
    "recommended ai voice agent for customer support",
    "recommended ai voice agent for marketing teams",
    "recommended ai voice agent for real estate",
    "recommended ai voice agent for recruiters",
    "recommended ai voice agent for hr teams",
    "recommended ai voice agent for healthcare",
    "recommended ai voice agent for finance teams",
    "recommended ai voice agent for travel companies",
    "recommended ai voice agent for developers",
    "recommended ai voice agent for local businesses",
    "recommended ai voice agent for startups 2026",
    "recommended ai voice agent in 2026",
    "recommended ai voice agent for remote teams",
    "affordable ai voice agent for startups",
    "affordable ai voice agent for small business",
    "affordable ai voice agent for saas",
    "affordable ai voice agent for agencies",
    "affordable ai voice agent for ecommerce",
    "affordable ai voice agent for sales teams",
    "affordable ai voice agent for b2b",
    "affordable ai voice agent for enterprises",
    "affordable ai voice agent for customer support",
    "affordable ai voice agent for marketing teams",
    "affordable ai voice agent for real estate",
    "affordable ai voice agent for recruiters",
    "affordable ai voice agent for hr teams",
    "affordable ai voice agent for healthcare",
    "affordable ai voice agent for finance teams",
    "affordable ai voice agent for travel companies",
    "affordable ai voice agent for developers",
    "affordable ai voice agent for local businesses",
    "affordable ai voice agent for startups 2026",
    "affordable ai voice agent in 2026",
    "affordable ai voice agent for remote teams",
    "enterprise ai voice agent for startups",
    "enterprise ai voice agent for small business",
    "enterprise ai voice agent for saas",
    "enterprise ai voice agent for agencies",
    "enterprise ai voice agent for ecommerce",
    "enterprise ai voice agent for sales teams",
    "enterprise ai voice agent for b2b",
    "enterprise ai voice agent for enterprises",
    "enterprise ai voice agent for customer support",
    "enterprise ai voice agent for marketing teams",
    "enterprise ai voice agent for real estate",
    "enterprise ai voice agent for recruiters",
    "enterprise ai voice agent for hr teams",
    "enterprise ai voice agent for healthcare",
    "enterprise ai voice agent for finance teams",
    "enterprise ai voice agent for travel companies",
    "enterprise ai voice agent for developers",
    "enterprise ai voice agent for local businesses",
    "enterprise ai voice agent for startups 2026",
    "enterprise ai voice agent in 2026",
    "enterprise ai voice agent for remote teams",
    "small business ai voice agent for startups",
    "small business ai voice agent for small business",
    "small business ai voice agent for saas",
    "small business ai voice agent for agencies",
    "small business ai voice agent for ecommerce",
    "small business ai voice agent for sales teams",
    "small business ai voice agent for b2b",
    "small business ai voice agent for enterprises",
    "small business ai voice agent for customer support",
    "small business ai voice agent for marketing teams",
    "small business ai voice agent for real estate",
    "small business ai voice agent for recruiters",
    "small business ai voice agent for hr teams",
    "small business ai voice agent for healthcare",
    "small business ai voice agent for finance teams",
    "small business ai voice agent for travel companies",
    "small business ai voice agent for developers",
    "small business ai voice agent for local businesses",
    "small business ai voice agent for startups 2026",
    "small business ai voice agent in 2026",
    "small business ai voice agent for remote teams",
    "startup ai voice agent for startups",
    "startup ai voice agent for small business",
    "startup ai voice agent for saas",
    "startup ai voice agent for agencies",
    "startup ai voice agent for ecommerce",
    "startup ai voice agent for sales teams",
    "startup ai voice agent for b2b",
    "startup ai voice agent for enterprises",
    "startup ai voice agent for customer support",
    "startup ai voice agent for marketing teams",
    "startup ai voice agent for real estate",
    "startup ai voice agent for recruiters",
    "startup ai voice agent for hr teams",
    "startup ai voice agent for healthcare",
    "startup ai voice agent for finance teams",
    "startup ai voice agent for travel companies",
    "startup ai voice agent for developers",
    "startup ai voice agent for local businesses",
    "startup ai voice agent for startups 2026",
    "startup ai voice agent in 2026",
    "startup ai voice agent for remote teams",
    "alternative ai voice agent for startups",
    "alternative ai voice agent for small business",
    "alternative ai voice agent for saas",
    "alternative ai voice agent for agencies",
    "alternative ai voice agent for ecommerce",
    "alternative ai voice agent for sales teams",
    "alternative ai voice agent for b2b",
    "alternative ai voice agent for enterprises",
    "alternative ai voice agent for customer support",
    "alternative ai voice agent for marketing teams",
    "alternative ai voice agent for real estate",
    "alternative ai voice agent for recruiters",
    "alternative ai voice agent for hr teams",
    "alternative ai voice agent for healthcare",
    "alternative ai voice agent for finance teams",
    "alternative ai voice agent for travel companies",
    "alternative ai voice agent for developers",
    "alternative ai voice agent for local businesses",
    "alternative ai voice agent for startups 2026",
    "alternative ai voice agent in 2026",
    "alternative ai voice agent for remote teams",
    "alternatives ai voice agent for startups",
    "alternatives ai voice agent for small business",
    "alternatives ai voice agent for saas",
    "alternatives ai voice agent for agencies",
    "alternatives ai voice agent for ecommerce",
    "alternatives ai voice agent for sales teams",
    "alternatives ai voice agent for b2b",
    "alternatives ai voice agent for enterprises",
    "alternatives ai voice agent for customer support",
    "alternatives ai voice agent for marketing teams",
    "alternatives ai voice agent for real estate",
    "alternatives ai voice agent for recruiters",
    "alternatives ai voice agent for hr teams",
    "alternatives ai voice agent for healthcare",
    "alternatives ai voice agent for finance teams",
    "alternatives ai voice agent for travel companies",
    "alternatives ai voice agent for developers",
    "alternatives ai voice agent for local businesses",
    "alternatives ai voice agent for startups 2026",
    "alternatives ai voice agent in 2026",
    "alternatives ai voice agent for remote teams",
    "comparison ai voice agent for startups",
    "comparison ai voice agent for small business",
    "comparison ai voice agent for saas",
    "comparison ai voice agent for agencies",
    "comparison ai voice agent for ecommerce",
    "comparison ai voice agent for sales teams",
    "comparison ai voice agent for b2b",
    "comparison ai voice agent for enterprises",
    "comparison ai voice agent for customer support",
    "comparison ai voice agent for marketing teams",
    "comparison ai voice agent for real estate",
    "comparison ai voice agent for recruiters",
    "comparison ai voice agent for hr teams",
    "comparison ai voice agent for healthcare",
    "comparison ai voice agent for finance teams",
    "comparison ai voice agent for travel companies",
    "comparison ai voice agent for developers",
    "comparison ai voice agent for local businesses",
    "comparison ai voice agent for startups 2026",
    "comparison ai voice agent in 2026",
    "comparison ai voice agent for remote teams",
    "vs ai voice agent for startups",
    "vs ai voice agent for small business",
    "vs ai voice agent for saas",
    "vs ai voice agent for agencies",
    "vs ai voice agent for ecommerce",
    "vs ai voice agent for sales teams",
    "vs ai voice agent for b2b",
    "vs ai voice agent for enterprises",
    "vs ai voice agent for customer support",
    "vs ai voice agent for marketing teams",
    "vs ai voice agent for real estate",
    "vs ai voice agent for recruiters",
    "vs ai voice agent for hr teams",
    "vs ai voice agent for healthcare",
    "vs ai voice agent for finance teams",
    "vs ai voice agent for travel companies",
    "vs ai voice agent for developers",
    "vs ai voice agent for local businesses",
    "vs ai voice agent for startups 2026",
    "vs ai voice agent in 2026",
    "vs ai voice agent for remote teams",
    "pricing ai voice agent for startups",
    "pricing ai voice agent for small business",
    "pricing ai voice agent for saas",
    "pricing ai voice agent for agencies",
    "pricing ai voice agent for ecommerce",
    "pricing ai voice agent for sales teams",
    "pricing ai voice agent for b2b",
    "pricing ai voice agent for enterprises",
    "pricing ai voice agent for customer support",
    "pricing ai voice agent for marketing teams",
    "pricing ai voice agent for real estate",
    "pricing ai voice agent for recruiters",
    "pricing ai voice agent for hr teams",
    "pricing ai voice agent for healthcare",
    "pricing ai voice agent for finance teams",
    "pricing ai voice agent for travel companies",
    "pricing ai voice agent for developers",
    "pricing ai voice agent for local businesses",
    "pricing ai voice agent for startups 2026",
    "pricing ai voice agent in 2026",
    "pricing ai voice agent for remote teams",
    "review ai voice agent for startups",
    "review ai voice agent for small business",
    "review ai voice agent for saas",
    "review ai voice agent for agencies",
    "review ai voice agent for ecommerce",
    "review ai voice agent for sales teams",
    "review ai voice agent for b2b",
    "review ai voice agent for enterprises",
    "review ai voice agent for customer support",
    "review ai voice agent for marketing teams",
    "review ai voice agent for real estate",
    "review ai voice agent for recruiters",
    "review ai voice agent for hr teams",
    "review ai voice agent for healthcare",
    "review ai voice agent for finance teams",
    "review ai voice agent for travel companies",
    "review ai voice agent for developers",
    "review ai voice agent for local businesses",
    "review ai voice agent for startups 2026",
    "review ai voice agent in 2026",
    "review ai voice agent for remote teams",
    "software ai voice agent for startups",
    "software ai voice agent for small business",
    "software ai voice agent for saas",
    "software ai voice agent for agencies",
    "software ai voice agent for ecommerce",
    "software ai voice agent for sales teams",
    "software ai voice agent for b2b",
    "software ai voice agent for enterprises",
    "software ai voice agent for customer support",
    "software ai voice agent for marketing teams",
    "software ai voice agent for real estate",
    "software ai voice agent for recruiters",
    "software ai voice agent for hr teams",
    "software ai voice agent for healthcare",
    "software ai voice agent for finance teams",
    "software ai voice agent for travel companies",
    "software ai voice agent for developers",
    "software ai voice agent for local businesses",
    "software ai voice agent for startups 2026",
    "software ai voice agent in 2026",
    "software ai voice agent for remote teams",
    "platform ai voice agent for startups",
    "platform ai voice agent for small business",
    "platform ai voice agent for saas",
    "platform ai voice agent for agencies",
    "platform ai voice agent for ecommerce",
    "platform ai voice agent for sales teams",
    "platform ai voice agent for b2b",
    "platform ai voice agent for enterprises",
    "platform ai voice agent for customer support",
    "platform ai voice agent for marketing teams",
    "platform ai voice agent for real estate",
    "platform ai voice agent for recruiters",
    "platform ai voice agent for hr teams",
    "platform ai voice agent for healthcare",
    "platform ai voice agent for finance teams",
    "platform ai voice agent for travel companies",
    "platform ai voice agent for developers",
    "platform ai voice agent for local businesses",
    "platform ai voice agent for startups 2026",
    "platform ai voice agent in 2026",
    "platform ai voice agent for remote teams",
    "solution ai voice agent for startups",
    "solution ai voice agent for small business",
    "solution ai voice agent for saas",
    "solution ai voice agent for agencies",
    "solution ai voice agent for ecommerce",
    "solution ai voice agent for sales teams",
    "solution ai voice agent for b2b",
    "solution ai voice agent for enterprises",
    "solution ai voice agent for customer support",
    "solution ai voice agent for marketing teams",
    "solution ai voice agent for real estate",
    "solution ai voice agent for recruiters",
    "solution ai voice agent for hr teams",
    "solution ai voice agent for healthcare",
    "solution ai voice agent for finance teams",
    "solution ai voice agent for travel companies",
    "solution ai voice agent for developers",
    "solution ai voice agent for local businesses",
    "solution ai voice agent for startups 2026",
    "solution ai voice agent in 2026",
    "solution ai voice agent for remote teams",
    "tool ai voice agent for startups",
    "tool ai voice agent for small business",
    "tool ai voice agent for saas",
    "tool ai voice agent for agencies",
    "tool ai voice agent for ecommerce",
    "tool ai voice agent for sales teams",
    "tool ai voice agent for b2b",
    "tool ai voice agent for enterprises",
    "tool ai voice agent for customer support",
    "tool ai voice agent for marketing teams",
    "tool ai voice agent for real estate",
    "tool ai voice agent for recruiters",
    "tool ai voice agent for hr teams",
    "tool ai voice agent for healthcare",
    "tool ai voice agent for finance teams",
    "tool ai voice agent for travel companies",
    "tool ai voice agent for developers",
    "tool ai voice agent for local businesses",
    "tool ai voice agent for startups 2026",
    "tool ai voice agent in 2026",
    "tool ai voice agent for remote teams",
    "tools ai voice agent for startups",
    "tools ai voice agent for small business",
    "tools ai voice agent for saas",
    "tools ai voice agent for agencies",
    "tools ai voice agent for ecommerce",
    "tools ai voice agent for sales teams",
    "tools ai voice agent for b2b",
    "tools ai voice agent for enterprises",
    "tools ai voice agent for customer support",
    "tools ai voice agent for marketing teams",
    "tools ai voice agent for real estate",
    "tools ai voice agent for recruiters",
    "tools ai voice agent for hr teams",
    "tools ai voice agent for healthcare",
    "tools ai voice agent for finance teams",
    "tools ai voice agent for travel companies",
    "tools ai voice agent for developers",
    "tools ai voice agent for local businesses",
    "tools ai voice agent for startups 2026",
    "tools ai voice agent in 2026",
    "tools ai voice agent for remote teams",
    "provider ai voice agent for startups",
    "provider ai voice agent for small business",
    "provider ai voice agent for saas",
    "provider ai voice agent for agencies",
    "provider ai voice agent for ecommerce",
    "provider ai voice agent for sales teams",
    "provider ai voice agent for b2b",
    "provider ai voice agent for enterprises",
    "provider ai voice agent for customer support",
    "provider ai voice agent for marketing teams",
    "provider ai voice agent for real estate",
    "provider ai voice agent for recruiters",
    "provider ai voice agent for hr teams",
    "provider ai voice agent for healthcare",
    "provider ai voice agent for finance teams",
    "provider ai voice agent for travel companies",
    "provider ai voice agent for developers",
    "provider ai voice agent for local businesses",
    "provider ai voice agent for startups 2026",
    "provider ai voice agent in 2026",
    "provider ai voice agent for remote teams",
    "service ai voice agent for startups",
    "service ai voice agent for small business",
    "service ai voice agent for saas",
    "service ai voice agent for agencies",
    "service ai voice agent for ecommerce",
    "service ai voice agent for sales teams",
    "service ai voice agent for b2b",
    "service ai voice agent for enterprises",
    "service ai voice agent for customer support",
    "service ai voice agent for marketing teams",
    "service ai voice agent for real estate",
    "service ai voice agent for recruiters",
    "service ai voice agent for hr teams",
    "service ai voice agent for healthcare",
    "service ai voice agent for finance teams",
    "service ai voice agent for travel companies",
    "service ai voice agent for developers",
    "service ai voice agent for local businesses",
    "service ai voice agent for startups 2026",
    "service ai voice agent in 2026",
    "service ai voice agent for remote teams",
    "automation ai voice agent for startups",
    "automation ai voice agent for small business",
    "automation ai voice agent for saas",
    "automation ai voice agent for agencies",
    "automation ai voice agent for ecommerce",
    "automation ai voice agent for sales teams",
    "automation ai voice agent for b2b",
    "automation ai voice agent for enterprises",
    "automation ai voice agent for customer support",
    "automation ai voice agent for marketing teams",
    "automation ai voice agent for real estate",
    "automation ai voice agent for recruiters",
    "automation ai voice agent for hr teams",
    "automation ai voice agent for healthcare",
    "automation ai voice agent for finance teams",
    "automation ai voice agent for travel companies",
    "automation ai voice agent for developers",
    "automation ai voice agent for local businesses",
    "automation ai voice agent for startups 2026",
    "automation ai voice agent in 2026",
    "automation ai voice agent for remote teams",
    "best ai voice agent",
    "how to use ai voice agent",
    "how to choose ai voice agent",
    "looking for ai voice agent",
    "ai voice agent recommendations",
    "ai voice agent alternatives",
    "ai voice agent comparison",
    "ai voice agent pricing",
    "ai voice agent review",
    "ai voice agent for lead generation",
    "ai voice agent for customer support",
    "ai voice agent for sales",
    "ai voice agent for marketing",
    "ai voice agent for business automation",
    "ai voice agent for appointment booking",
    "ai voice agent for outbound outreach",
    "ai voice agent for ecommerce",
    "ai voice agent for workflow automation",
    "ai voice agent for research",
    "ai voice agent for business",
    "need ai voice agent",
    "recommend a ai voice agent",
    "what is the best ai voice agent",
    "which ai voice agent should i use",
    "best ai marketing agent for startups",
    "best ai marketing agent for small business",
    "best ai marketing agent for saas",
    "best ai marketing agent for agencies",
    "best ai marketing agent for ecommerce",
    "best ai marketing agent for sales teams",
    "best ai marketing agent for b2b",
    "best ai marketing agent for enterprises",
    "best ai marketing agent for customer support",
    "best ai marketing agent for marketing teams",
    "best ai marketing agent for real estate",
    "best ai marketing agent for recruiters",
    "best ai marketing agent for hr teams",
    "best ai marketing agent for healthcare",
    "best ai marketing agent for finance teams",
    "best ai marketing agent for travel companies",
    "best ai marketing agent for developers",
    "best ai marketing agent for local businesses",
    "best ai marketing agent for startups 2026",
    "best ai marketing agent in 2026",
    "best ai marketing agent for remote teams",
    "top ai marketing agent for startups",
    "top ai marketing agent for small business",
    "top ai marketing agent for saas",
    "top ai marketing agent for agencies",
    "top ai marketing agent for ecommerce",
    "top ai marketing agent for sales teams",
    "top ai marketing agent for b2b",
    "top ai marketing agent for enterprises",
    "top ai marketing agent for customer support",
    "top ai marketing agent for marketing teams",
    "top ai marketing agent for real estate",
    "top ai marketing agent for recruiters",
    "top ai marketing agent for hr teams",
    "top ai marketing agent for healthcare",
    "top ai marketing agent for finance teams",
    "top ai marketing agent for travel companies",
    "top ai marketing agent for developers",
    "top ai marketing agent for local businesses",
    "top ai marketing agent for startups 2026",
    "top ai marketing agent in 2026",
    "top ai marketing agent for remote teams",
    "recommended ai marketing agent for startups",
    "recommended ai marketing agent for small business",
    "recommended ai marketing agent for saas",
    "recommended ai marketing agent for agencies",
    "recommended ai marketing agent for ecommerce",
    "recommended ai marketing agent for sales teams",
    "recommended ai marketing agent for b2b",
    "recommended ai marketing agent for enterprises",
    "recommended ai marketing agent for customer support",
    "recommended ai marketing agent for marketing teams",
    "recommended ai marketing agent for real estate",
    "recommended ai marketing agent for recruiters",
    "recommended ai marketing agent for hr teams",
    "recommended ai marketing agent for healthcare",
    "recommended ai marketing agent for finance teams",
    "recommended ai marketing agent for travel companies",
    "recommended ai marketing agent for developers",
    "recommended ai marketing agent for local businesses",
    "recommended ai marketing agent for startups 2026",
    "recommended ai marketing agent in 2026",
    "recommended ai marketing agent for remote teams",
    "affordable ai marketing agent for startups",
    "affordable ai marketing agent for small business",
    "affordable ai marketing agent for saas",
    "affordable ai marketing agent for agencies",
    "affordable ai marketing agent for ecommerce",
    "affordable ai marketing agent for sales teams",
    "affordable ai marketing agent for b2b",
    "affordable ai marketing agent for enterprises",
    "affordable ai marketing agent for customer support",
    "affordable ai marketing agent for marketing teams",
    "affordable ai marketing agent for real estate",
    "affordable ai marketing agent for recruiters",
    "affordable ai marketing agent for hr teams",
    "affordable ai marketing agent for healthcare",
    "affordable ai marketing agent for finance teams",
    "affordable ai marketing agent for travel companies",
    "affordable ai marketing agent for developers",
    "affordable ai marketing agent for local businesses",
    "affordable ai marketing agent for startups 2026",
    "affordable ai marketing agent in 2026",
    "affordable ai marketing agent for remote teams",
    "enterprise ai marketing agent for startups",
    "enterprise ai marketing agent for small business",
    "enterprise ai marketing agent for saas",
    "enterprise ai marketing agent for agencies",
    "enterprise ai marketing agent for ecommerce",
    "enterprise ai marketing agent for sales teams",
    "enterprise ai marketing agent for b2b",
    "enterprise ai marketing agent for enterprises",
    "enterprise ai marketing agent for customer support",
    "enterprise ai marketing agent for marketing teams",
    "enterprise ai marketing agent for real estate",
    "enterprise ai marketing agent for recruiters",
    "enterprise ai marketing agent for hr teams",
    "enterprise ai marketing agent for healthcare",
    "enterprise ai marketing agent for finance teams",
    "enterprise ai marketing agent for travel companies",
    "enterprise ai marketing agent for developers",
    "enterprise ai marketing agent for local businesses",
    "enterprise ai marketing agent for startups 2026",
    "enterprise ai marketing agent in 2026",
    "enterprise ai marketing agent for remote teams",
    "small business ai marketing agent for startups",
    "small business ai marketing agent for small business",
    "small business ai marketing agent for saas",
    "small business ai marketing agent for agencies",
    "small business ai marketing agent for ecommerce",
    "small business ai marketing agent for sales teams",
    "small business ai marketing agent for b2b",
    "small business ai marketing agent for enterprises",
    "small business ai marketing agent for customer support",
    "small business ai marketing agent for marketing teams",
    "small business ai marketing agent for real estate",
    "small business ai marketing agent for recruiters",
    "small business ai marketing agent for hr teams",
    "small business ai marketing agent for healthcare",
    "small business ai marketing agent for finance teams",
    "small business ai marketing agent for travel companies",
    "small business ai marketing agent for developers",
    "small business ai marketing agent for local businesses",
    "small business ai marketing agent for startups 2026",
    "small business ai marketing agent in 2026",
    "small business ai marketing agent for remote teams",
    "startup ai marketing agent for startups",
    "startup ai marketing agent for small business",
    "startup ai marketing agent for saas",
    "startup ai marketing agent for agencies",
    "startup ai marketing agent for ecommerce",
    "startup ai marketing agent for sales teams",
    "startup ai marketing agent for b2b",
    "startup ai marketing agent for enterprises",
    "startup ai marketing agent for customer support",
    "startup ai marketing agent for marketing teams",
    "startup ai marketing agent for real estate",
    "startup ai marketing agent for recruiters",
    "startup ai marketing agent for hr teams",
    "startup ai marketing agent for healthcare",
    "startup ai marketing agent for finance teams",
    "startup ai marketing agent for travel companies",
    "startup ai marketing agent for developers",
    "startup ai marketing agent for local businesses",
    "startup ai marketing agent for startups 2026",
    "startup ai marketing agent in 2026",
    "startup ai marketing agent for remote teams",
    "alternative ai marketing agent for startups",
    "alternative ai marketing agent for small business",
    "alternative ai marketing agent for saas",
    "alternative ai marketing agent for agencies",
    "alternative ai marketing agent for ecommerce",
    "alternative ai marketing agent for sales teams",
    "alternative ai marketing agent for b2b",
    "alternative ai marketing agent for enterprises",
    "alternative ai marketing agent for customer support",
    "alternative ai marketing agent for marketing teams",
    "alternative ai marketing agent for real estate",
    "alternative ai marketing agent for recruiters",
    "alternative ai marketing agent for hr teams",
    "alternative ai marketing agent for healthcare",
    "alternative ai marketing agent for finance teams",
    "alternative ai marketing agent for travel companies",
    "alternative ai marketing agent for developers",
    "alternative ai marketing agent for local businesses",
    "alternative ai marketing agent for startups 2026",
    "alternative ai marketing agent in 2026",
    "alternative ai marketing agent for remote teams",
    "alternatives ai marketing agent for startups",
    "alternatives ai marketing agent for small business",
    "alternatives ai marketing agent for saas",
    "alternatives ai marketing agent for agencies",
    "alternatives ai marketing agent for ecommerce",
    "alternatives ai marketing agent for sales teams",
    "alternatives ai marketing agent for b2b",
    "alternatives ai marketing agent for enterprises",
    "alternatives ai marketing agent for customer support",
    "alternatives ai marketing agent for marketing teams",
    "alternatives ai marketing agent for real estate",
    "alternatives ai marketing agent for recruiters",
    "alternatives ai marketing agent for hr teams",
    "alternatives ai marketing agent for healthcare",
    "alternatives ai marketing agent for finance teams",
    "alternatives ai marketing agent for travel companies",
    "alternatives ai marketing agent for developers",
    "alternatives ai marketing agent for local businesses",
    "alternatives ai marketing agent for startups 2026",
    "alternatives ai marketing agent in 2026",
    "alternatives ai marketing agent for remote teams",
    "comparison ai marketing agent for startups",
    "comparison ai marketing agent for small business",
    "comparison ai marketing agent for saas",
    "comparison ai marketing agent for agencies",
    "comparison ai marketing agent for ecommerce",
    "comparison ai marketing agent for sales teams",
    "comparison ai marketing agent for b2b",
    "comparison ai marketing agent for enterprises",
    "comparison ai marketing agent for customer support",
    "comparison ai marketing agent for marketing teams",
    "comparison ai marketing agent for real estate",
    "comparison ai marketing agent for recruiters",
    "comparison ai marketing agent for hr teams",
    "comparison ai marketing agent for healthcare",
    "comparison ai marketing agent for finance teams",
    "comparison ai marketing agent for travel companies",
    "comparison ai marketing agent for developers",
    "comparison ai marketing agent for local businesses",
    "comparison ai marketing agent for startups 2026",
    "comparison ai marketing agent in 2026",
    "comparison ai marketing agent for remote teams",
    "vs ai marketing agent for startups",
    "vs ai marketing agent for small business",
    "vs ai marketing agent for saas",
    "vs ai marketing agent for agencies",
    "vs ai marketing agent for ecommerce",
    "vs ai marketing agent for sales teams",
    "vs ai marketing agent for b2b",
    "vs ai marketing agent for enterprises",
    "vs ai marketing agent for customer support",
    "vs ai marketing agent for marketing teams",
    "vs ai marketing agent for real estate",
    "vs ai marketing agent for recruiters",
    "vs ai marketing agent for hr teams",
    "vs ai marketing agent for healthcare",
    "vs ai marketing agent for finance teams",
    "vs ai marketing agent for travel companies",
    "vs ai marketing agent for developers",
    "vs ai marketing agent for local businesses",
    "vs ai marketing agent for startups 2026",
    "vs ai marketing agent in 2026",
    "vs ai marketing agent for remote teams",
    "pricing ai marketing agent for startups",
    "pricing ai marketing agent for small business",
    "pricing ai marketing agent for saas",
    "pricing ai marketing agent for agencies",
    "pricing ai marketing agent for ecommerce",
    "pricing ai marketing agent for sales teams",
    "pricing ai marketing agent for b2b",
    "pricing ai marketing agent for enterprises",
    "pricing ai marketing agent for customer support",
    "pricing ai marketing agent for marketing teams",
    "pricing ai marketing agent for real estate",
    "pricing ai marketing agent for recruiters",
    "pricing ai marketing agent for hr teams",
    "pricing ai marketing agent for healthcare",
    "pricing ai marketing agent for finance teams",
    "pricing ai marketing agent for travel companies",
    "pricing ai marketing agent for developers",
    "pricing ai marketing agent for local businesses",
    "pricing ai marketing agent for startups 2026",
    "pricing ai marketing agent in 2026",
    "pricing ai marketing agent for remote teams",
    "review ai marketing agent for startups",
    "review ai marketing agent for small business",
    "review ai marketing agent for saas",
    "review ai marketing agent for agencies",
    "review ai marketing agent for ecommerce",
    "review ai marketing agent for sales teams",
    "review ai marketing agent for b2b",
    "review ai marketing agent for enterprises",
    "review ai marketing agent for customer support",
    "review ai marketing agent for marketing teams",
    "review ai marketing agent for real estate",
    "review ai marketing agent for recruiters",
    "review ai marketing agent for hr teams",
    "review ai marketing agent for healthcare",
    "review ai marketing agent for finance teams",
    "review ai marketing agent for travel companies",
    "review ai marketing agent for developers",
    "review ai marketing agent for local businesses",
    "review ai marketing agent for startups 2026",
    "review ai marketing agent in 2026",
    "review ai marketing agent for remote teams",
    "software ai marketing agent for startups",
    "software ai marketing agent for small business",
    "software ai marketing agent for saas",
    "software ai marketing agent for agencies",
    "software ai marketing agent for ecommerce",
    "software ai marketing agent for sales teams",
    "software ai marketing agent for b2b",
    "software ai marketing agent for enterprises",
    "software ai marketing agent for customer support",
    "software ai marketing agent for marketing teams",
    "software ai marketing agent for real estate",
    "software ai marketing agent for recruiters",
    "software ai marketing agent for hr teams",
    "software ai marketing agent for healthcare",
    "software ai marketing agent for finance teams",
    "software ai marketing agent for travel companies",
    "software ai marketing agent for developers",
    "software ai marketing agent for local businesses",
    "software ai marketing agent for startups 2026",
    "software ai marketing agent in 2026",
    "software ai marketing agent for remote teams",
    "platform ai marketing agent for startups",
    "platform ai marketing agent for small business",
    "platform ai marketing agent for saas",
    "platform ai marketing agent for agencies",
    "platform ai marketing agent for ecommerce",
    "platform ai marketing agent for sales teams",
    "platform ai marketing agent for b2b",
    "platform ai marketing agent for enterprises",
    "platform ai marketing agent for customer support",
    "platform ai marketing agent for marketing teams",
    "platform ai marketing agent for real estate",
    "platform ai marketing agent for recruiters",
    "platform ai marketing agent for hr teams",
    "platform ai marketing agent for healthcare",
    "platform ai marketing agent for finance teams",
    "platform ai marketing agent for travel companies",
    "platform ai marketing agent for developers",
    "platform ai marketing agent for local businesses",
    "platform ai marketing agent for startups 2026",
    "platform ai marketing agent in 2026",
    "platform ai marketing agent for remote teams",
    "solution ai marketing agent for startups",
    "solution ai marketing agent for small business",
    "solution ai marketing agent for saas",
    "solution ai marketing agent for agencies",
    "solution ai marketing agent for ecommerce",
    "solution ai marketing agent for sales teams",
    "solution ai marketing agent for b2b",
    "solution ai marketing agent for enterprises",
    "solution ai marketing agent for customer support",
    "solution ai marketing agent for marketing teams",
    "solution ai marketing agent for real estate",
    "solution ai marketing agent for recruiters",
    "solution ai marketing agent for hr teams",
    "solution ai marketing agent for healthcare",
    "solution ai marketing agent for finance teams",
    "solution ai marketing agent for travel companies",
    "solution ai marketing agent for developers",
    "solution ai marketing agent for local businesses",
    "solution ai marketing agent for startups 2026",
    "solution ai marketing agent in 2026",
    "solution ai marketing agent for remote teams",
    "tool ai marketing agent for startups",
    "tool ai marketing agent for small business",
    "tool ai marketing agent for saas",
    "tool ai marketing agent for agencies",
    "tool ai marketing agent for ecommerce",
    "tool ai marketing agent for sales teams",
    "tool ai marketing agent for b2b",
    "tool ai marketing agent for enterprises",
    "tool ai marketing agent for customer support",
    "tool ai marketing agent for marketing teams",
    "tool ai marketing agent for real estate",
    "tool ai marketing agent for recruiters",
    "tool ai marketing agent for hr teams",
    "tool ai marketing agent for healthcare",
    "tool ai marketing agent for finance teams",
    "tool ai marketing agent for travel companies",
    "tool ai marketing agent for developers",
    "tool ai marketing agent for local businesses",
    "tool ai marketing agent for startups 2026",
    "tool ai marketing agent in 2026",
    "tool ai marketing agent for remote teams",
    "tools ai marketing agent for startups",
    "tools ai marketing agent for small business",
    "tools ai marketing agent for saas",
    "tools ai marketing agent for agencies",
    "tools ai marketing agent for ecommerce",
    "tools ai marketing agent for sales teams",
    "tools ai marketing agent for b2b",
    "tools ai marketing agent for enterprises",
    "tools ai marketing agent for customer support",
    "tools ai marketing agent for marketing teams",
    "tools ai marketing agent for real estate",
    "tools ai marketing agent for recruiters",
    "tools ai marketing agent for hr teams",
    "tools ai marketing agent for healthcare",
    "tools ai marketing agent for finance teams",
    "tools ai marketing agent for travel companies",
    "tools ai marketing agent for developers",
    "tools ai marketing agent for local businesses",
    "tools ai marketing agent for startups 2026",
    "tools ai marketing agent in 2026",
    "tools ai marketing agent for remote teams",
    "provider ai marketing agent for startups",
    "provider ai marketing agent for small business",
    "provider ai marketing agent for saas",
    "provider ai marketing agent for agencies",
    "provider ai marketing agent for ecommerce",
    "provider ai marketing agent for sales teams",
    "provider ai marketing agent for b2b",
    "provider ai marketing agent for enterprises",
    "provider ai marketing agent for customer support",
    "provider ai marketing agent for marketing teams",
    "provider ai marketing agent for real estate",
    "provider ai marketing agent for recruiters",
    "provider ai marketing agent for hr teams",
    "provider ai marketing agent for healthcare",
    "provider ai marketing agent for finance teams",
    "provider ai marketing agent for travel companies",
    "provider ai marketing agent for developers",
    "provider ai marketing agent for local businesses",
    "provider ai marketing agent for startups 2026",
    "provider ai marketing agent in 2026",
    "provider ai marketing agent for remote teams",
    "service ai marketing agent for startups",
    "service ai marketing agent for small business",
    "service ai marketing agent for saas",
    "service ai marketing agent for agencies",
    "service ai marketing agent for ecommerce",
    "service ai marketing agent for sales teams",
    "service ai marketing agent for b2b",
    "service ai marketing agent for enterprises",
    "service ai marketing agent for customer support",
    "service ai marketing agent for marketing teams",
    "service ai marketing agent for real estate",
    "service ai marketing agent for recruiters",
    "service ai marketing agent for hr teams",
    "service ai marketing agent for healthcare",
    "service ai marketing agent for finance teams",
    "service ai marketing agent for travel companies",
    "service ai marketing agent for developers",
    "service ai marketing agent for local businesses",
    "service ai marketing agent for startups 2026",
    "service ai marketing agent in 2026",
    "service ai marketing agent for remote teams",
    "automation ai marketing agent for startups",
    "automation ai marketing agent for small business",
    "automation ai marketing agent for saas",
    "automation ai marketing agent for agencies",
    "automation ai marketing agent for ecommerce",
    "automation ai marketing agent for sales teams",
    "automation ai marketing agent for b2b",
    "automation ai marketing agent for enterprises",
    "automation ai marketing agent for customer support",
    "automation ai marketing agent for marketing teams",
    "automation ai marketing agent for real estate",
    "automation ai marketing agent for recruiters",
    "automation ai marketing agent for hr teams",
    "automation ai marketing agent for healthcare",
    "automation ai marketing agent for finance teams",
    "automation ai marketing agent for travel companies",
    "automation ai marketing agent for developers",
    "automation ai marketing agent for local businesses",
    "automation ai marketing agent for startups 2026",
    "automation ai marketing agent in 2026",
    "automation ai marketing agent for remote teams",
    "best ai marketing agent",
    "how to use ai marketing agent",
    "how to choose ai marketing agent",
    "looking for ai marketing agent",
    "ai marketing agent recommendations",
    "ai marketing agent alternatives",
    "ai marketing agent comparison",
    "ai marketing agent pricing",
    "ai marketing agent review",
    "ai marketing agent for lead generation",
    "ai marketing agent for customer support",
    "ai marketing agent for sales",
    "ai marketing agent for marketing",
    "ai marketing agent for business automation",
    "ai marketing agent for appointment booking",
    "ai marketing agent for outbound outreach",
    "ai marketing agent for ecommerce",
    "ai marketing agent for workflow automation",
    "ai marketing agent for research",
    "ai marketing agent for business",
    "need ai marketing agent",
    "recommend a ai marketing agent",
    "what is the best ai marketing agent",
    "which ai marketing agent should i use",
    "best ai lead generation agent for startups",
    "best ai lead generation agent for small business",
    "best ai lead generation agent for saas",
    "best ai lead generation agent for agencies",
    "best ai lead generation agent for ecommerce",
    "best ai lead generation agent for sales teams",
    "best ai lead generation agent for b2b",
    "best ai lead generation agent for enterprises",
    "best ai lead generation agent for customer support",
    "best ai lead generation agent for marketing teams",
    "best ai lead generation agent for real estate",
    "best ai lead generation agent for recruiters",
    "best ai lead generation agent for hr teams",
    "best ai lead generation agent for healthcare",
    "best ai lead generation agent for finance teams",
    "best ai lead generation agent for travel companies",
    "best ai lead generation agent for developers",
    "best ai lead generation agent for local businesses",
    "best ai lead generation agent for startups 2026",
    "best ai lead generation agent in 2026",
    "best ai lead generation agent for remote teams",
    "top ai lead generation agent for startups",
    "top ai lead generation agent for small business",
    "top ai lead generation agent for saas",
    "top ai lead generation agent for agencies",
    "top ai lead generation agent for ecommerce",
    "top ai lead generation agent for sales teams",
    "top ai lead generation agent for b2b",
    "top ai lead generation agent for enterprises",
    "top ai lead generation agent for customer support",
    "top ai lead generation agent for marketing teams",
    "top ai lead generation agent for real estate",
    "top ai lead generation agent for recruiters",
    "top ai lead generation agent for hr teams",
    "top ai lead generation agent for healthcare",
    "top ai lead generation agent for finance teams",
    "top ai lead generation agent for travel companies",
    "top ai lead generation agent for developers",
    "top ai lead generation agent for local businesses",
    "top ai lead generation agent for startups 2026",
    "top ai lead generation agent in 2026",
    "top ai lead generation agent for remote teams",
    "recommended ai lead generation agent for startups",
    "recommended ai lead generation agent for small business",
    "recommended ai lead generation agent for saas",
    "recommended ai lead generation agent for agencies",
    "recommended ai lead generation agent for ecommerce",
    "recommended ai lead generation agent for sales teams",
    "recommended ai lead generation agent for b2b",
    "recommended ai lead generation agent for enterprises",
    "recommended ai lead generation agent for customer support",
    "recommended ai lead generation agent for marketing teams",
    "recommended ai lead generation agent for real estate",
    "recommended ai lead generation agent for recruiters",
    "recommended ai lead generation agent for hr teams",
    "recommended ai lead generation agent for healthcare",
    "recommended ai lead generation agent for finance teams",
    "recommended ai lead generation agent for travel companies",
    "recommended ai lead generation agent for developers",
    "recommended ai lead generation agent for local businesses",
    "recommended ai lead generation agent for startups 2026",
    "recommended ai lead generation agent in 2026",
    "recommended ai lead generation agent for remote teams",
    "affordable ai lead generation agent for startups",
    "affordable ai lead generation agent for small business",
    "affordable ai lead generation agent for saas",
    "affordable ai lead generation agent for agencies",
    "affordable ai lead generation agent for ecommerce",
    "affordable ai lead generation agent for sales teams",
    "affordable ai lead generation agent for b2b",
    "affordable ai lead generation agent for enterprises",
    "affordable ai lead generation agent for customer support",
    "affordable ai lead generation agent for marketing teams",
    "affordable ai lead generation agent for real estate",
    "affordable ai lead generation agent for recruiters",
    "affordable ai lead generation agent for hr teams",
    "affordable ai lead generation agent for healthcare",
    "affordable ai lead generation agent for finance teams",
    "affordable ai lead generation agent for travel companies",
    "affordable ai lead generation agent for developers",
    "affordable ai lead generation agent for local businesses",
    "affordable ai lead generation agent for startups 2026",
    "affordable ai lead generation agent in 2026",
    "affordable ai lead generation agent for remote teams",
    "enterprise ai lead generation agent for startups",
    "enterprise ai lead generation agent for small business",
    "enterprise ai lead generation agent for saas",
    "enterprise ai lead generation agent for agencies",
    "enterprise ai lead generation agent for ecommerce",
    "enterprise ai lead generation agent for sales teams",
    "enterprise ai lead generation agent for b2b",
    "enterprise ai lead generation agent for enterprises",
    "enterprise ai lead generation agent for customer support",
    "enterprise ai lead generation agent for marketing teams",
    "enterprise ai lead generation agent for real estate",
    "enterprise ai lead generation agent for recruiters",
    "enterprise ai lead generation agent for hr teams",
    "enterprise ai lead generation agent for healthcare",
    "enterprise ai lead generation agent for finance teams",
    "enterprise ai lead generation agent for travel companies",
    "enterprise ai lead generation agent for developers",
    "enterprise ai lead generation agent for local businesses",
    "enterprise ai lead generation agent for startups 2026",
    "enterprise ai lead generation agent in 2026",
    "enterprise ai lead generation agent for remote teams",
    "small business ai lead generation agent for startups",
    "small business ai lead generation agent for small business",
    "small business ai lead generation agent for saas",
    "small business ai lead generation agent for agencies",
    "small business ai lead generation agent for ecommerce",
    "small business ai lead generation agent for sales teams",
    "small business ai lead generation agent for b2b",
    "small business ai lead generation agent for enterprises",
    "small business ai lead generation agent for customer support",
    "small business ai lead generation agent for marketing teams",
    "small business ai lead generation agent for real estate",
    "small business ai lead generation agent for recruiters",
    "small business ai lead generation agent for hr teams",
    "small business ai lead generation agent for healthcare",
    "small business ai lead generation agent for finance teams",
    "small business ai lead generation agent for travel companies",
    "small business ai lead generation agent for developers",
    "small business ai lead generation agent for local businesses",
    "small business ai lead generation agent for startups 2026",
    "small business ai lead generation agent in 2026",
    "small business ai lead generation agent for remote teams",
    "startup ai lead generation agent for startups",
    "startup ai lead generation agent for small business",
    "startup ai lead generation agent for saas",
    "startup ai lead generation agent for agencies",
    "startup ai lead generation agent for ecommerce",
    "startup ai lead generation agent for sales teams",
    "startup ai lead generation agent for b2b",
    "startup ai lead generation agent for enterprises",
    "startup ai lead generation agent for customer support",
    "startup ai lead generation agent for marketing teams",
    "startup ai lead generation agent for real estate",
    "startup ai lead generation agent for recruiters",
    "startup ai lead generation agent for hr teams",
    "startup ai lead generation agent for healthcare",
    "startup ai lead generation agent for finance teams",
    "startup ai lead generation agent for travel companies",
    "startup ai lead generation agent for developers",
    "startup ai lead generation agent for local businesses",
    "startup ai lead generation agent for startups 2026",
    "startup ai lead generation agent in 2026",
    "startup ai lead generation agent for remote teams",
    "alternative ai lead generation agent for startups",
    "alternative ai lead generation agent for small business",
    "alternative ai lead generation agent for saas",
    "alternative ai lead generation agent for agencies",
    "alternative ai lead generation agent for ecommerce",
    "alternative ai lead generation agent for sales teams",
    "alternative ai lead generation agent for b2b",
    "alternative ai lead generation agent for enterprises",
    "alternative ai lead generation agent for customer support",
    "alternative ai lead generation agent for marketing teams",
    "alternative ai lead generation agent for real estate",
    "alternative ai lead generation agent for recruiters",
    "alternative ai lead generation agent for hr teams",
    "alternative ai lead generation agent for healthcare",
    "alternative ai lead generation agent for finance teams",
    "alternative ai lead generation agent for travel companies",
    "alternative ai lead generation agent for developers",
    "alternative ai lead generation agent for local businesses",
    "alternative ai lead generation agent for startups 2026",
    "alternative ai lead generation agent in 2026",
    "alternative ai lead generation agent for remote teams",
    "alternatives ai lead generation agent for startups",
    "alternatives ai lead generation agent for small business",
    "alternatives ai lead generation agent for saas",
    "alternatives ai lead generation agent for agencies",
    "alternatives ai lead generation agent for ecommerce",
    "alternatives ai lead generation agent for sales teams",
    "alternatives ai lead generation agent for b2b",
    "alternatives ai lead generation agent for enterprises",
    "alternatives ai lead generation agent for customer support",
    "alternatives ai lead generation agent for marketing teams",
    "alternatives ai lead generation agent for real estate",
    "alternatives ai lead generation agent for recruiters",
    "alternatives ai lead generation agent for hr teams",
    "alternatives ai lead generation agent for healthcare",
    "alternatives ai lead generation agent for finance teams",
    "alternatives ai lead generation agent for travel companies",
    "alternatives ai lead generation agent for developers",
    "alternatives ai lead generation agent for local businesses",
    "alternatives ai lead generation agent for startups 2026",
    "alternatives ai lead generation agent in 2026",
    "alternatives ai lead generation agent for remote teams",
    "comparison ai lead generation agent for startups",
    "comparison ai lead generation agent for small business",
    "comparison ai lead generation agent for saas",
    "comparison ai lead generation agent for agencies",
    "comparison ai lead generation agent for ecommerce",
    "comparison ai lead generation agent for sales teams",
    "comparison ai lead generation agent for b2b",
    "comparison ai lead generation agent for enterprises",
    "comparison ai lead generation agent for customer support",
    "comparison ai lead generation agent for marketing teams",
    "comparison ai lead generation agent for real estate",
    "comparison ai lead generation agent for recruiters",
    "comparison ai lead generation agent for hr teams",
    "comparison ai lead generation agent for healthcare",
    "comparison ai lead generation agent for finance teams",
    "comparison ai lead generation agent for travel companies",
    "comparison ai lead generation agent for developers",
    "comparison ai lead generation agent for local businesses",
    "comparison ai lead generation agent for startups 2026",
    "comparison ai lead generation agent in 2026",
    "comparison ai lead generation agent for remote teams"

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
#
# "due" and "missing volume" are determined PURELY from flintel_keywords
# itself (fetched=False / search_volume=None on the stored document) —
# NOT from whether the keyword still happens to be present in the
# REDDIT_SEARCH_KEYWORDS python list above. The python list's only job is
# to tell sync_keywords_to_db() which brand-new keywords to INSERT
# (insert-only, via $setOnInsert — never overwrites an existing doc).
KEYWORD_CHECK_INTERVAL_SECONDS  = int(os.getenv("KEYWORD_CHECK_INTERVAL_SECONDS", "60"))

# ── KEYWORD RETRY COOLDOWN (kept, unchanged from v9.11.2) ───────────────────
# NOTE: as of v9.12, process_one_keyword() no longer fetches Reddit posts
# itself (that now happens in the fully separate
# run_google_posts_rss_matching_loop() below, driven off flintel_google_posts,
# not off a per-keyword failure). This cooldown mechanism and
# set_keyword_retry_cooldown() are kept 100% as-is for API compatibility
# and in case of future SERP-call-level failures, but process_one_keyword()
# no longer produces had_fetch_failure=True from a Reddit RSS failure —
# see process_one_keyword() below for what "had_fetch_failure" means now.
REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS = int(os.getenv("REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS", "1800"))

SERP_RESULTS_PER_KEYWORD = int(os.getenv("SERP_RESULTS_PER_KEYWORD", "20"))
SERP_MONTHS_BACK         = int(os.getenv("SERP_MONTHS_BACK", "6"))
SERP_FETCH_SLEEP_SECONDS = float(os.getenv("SERP_FETCH_SLEEP_SECONDS", "1.5"))

# ── SEARCH-VOLUME BATCH SEEDING CONFIG ──────────────────────────────────────
# search_volume/live bills PER REQUEST, not per keyword, and accepts up to
# 1000 keywords in a single call. We use 500 as a safe default chunk size.
SEARCH_VOLUME_BATCH_SIZE = int(os.getenv("SEARCH_VOLUME_BATCH_SIZE", "12"))

# ── FLINTEL_GOOGLE_POSTS / RSS-MATCHING CONFIG (NEW in v9.12) ───────────────
# GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS -> how often the independent
#   Reddit-RSS-matching loop wakes up to re-read flintel_google_posts for
#   distinct subreddits that still have fetched=False documents. This is a
#   cheap DB query — the actual per-subreddit RSS HTTP call only fires for
#   subreddits that genuinely have pending (fetched=False) documents.
#
# FUZZY_KEYWORDS_PER_POST -> how many auto-generated fuzzy keyword variants
#   generate_fuzzy_keywords() produces per discovered Google-SERP post
#   (6-7 by default, smart word-combination based off the matched Google
#   search keyword — see generate_fuzzy_keywords()).
GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS = int(os.getenv("GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS", "45"))
FUZZY_KEYWORDS_PER_POST = int(os.getenv("FUZZY_KEYWORDS_PER_POST", "7"))
GOOGLE_POSTS_RSS_ENTRY_LIMIT = int(os.getenv("GOOGLE_POSTS_RSS_ENTRY_LIMIT", "40"))

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
# Governs the retry/backoff/User-Agent behaviour of _reddit_get_with_retry()
# — used both for the per-subreddit RSS fetch (v9.12) — public,
# credential-free, no OAuth/PRAW. Does NOT change what data is extracted or
# where it goes — only how reliably we get a 200 instead of a 403 from
# Reddit's public RSS feeds.
REDDIT_FETCH_MAX_RETRIES     = int(os.getenv("REDDIT_FETCH_MAX_RETRIES", "3"))
REDDIT_FETCH_BACKOFF_BASE    = float(os.getenv("REDDIT_FETCH_BACKOFF_BASE", "2.0"))
REDDIT_FETCH_JITTER_MIN      = float(os.getenv("REDDIT_FETCH_JITTER_MIN", "0.4"))
REDDIT_FETCH_JITTER_MAX      = float(os.getenv("REDDIT_FETCH_JITTER_MAX", "1.6"))
# Reddit recommends: "<platform>:<app id>:<version> (by /u/<username>)"
REDDIT_USER_AGENT = os.getenv(
    "REDDIT_USER_AGENT",
    "python:flintel-signal-bot:v9.12 (by /u/flintel_signals)",
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
# FUZZY KEYWORD GENERATION (NEW in v9.12)
#
# Given the exact Google search keyword that produced a SERP result,
# deterministically generates 6-7 "fuzzy" keyword variants using smart
# word-combination logic — contiguous n-grams (bigrams/trigrams),
# stopword-stripped phrases, partial (head/tail-trimmed) phrases, and
# individually significant single words. No external NLP library is
# needed — this is a pure, dependency-free, reproducible heuristic.
#
# These fuzzy keywords are stored alongside each flintel_google_posts
# document purely for traceability / secondary text confirmation when
# run_google_posts_rss_matching_loop() later polls that post's subreddit
# via RSS — the AUTHORITATIVE match signal is always the exact post_url,
# never the fuzzy keywords alone (see that function for details).
# ─────────────────────────────────────────────────────────────────────────────

_FUZZY_STOPWORDS = {
    "a", "an", "the", "my", "our", "your", "their", "his", "her",
    "to", "for", "is", "are", "was", "were", "of", "on", "in", "it",
    "this", "that", "and", "or", "with", "at", "by", "from", "as",
    "be", "been", "has", "have", "had", "do", "does", "did", "not",
}


def generate_fuzzy_keywords(keyword: str, max_variants: int = FUZZY_KEYWORDS_PER_POST) -> list:
    """
    Deterministically generates up to `max_variants` fuzzy keyword
    strings from `keyword` (the exact Google search keyword that produced
    a given SERP result). Smart, dependency-free, word-combination based:

      - the full original phrase (lowercased)
      - the stopword-stripped content-word phrase
      - every contiguous bigram
      - every contiguous trigram (if the phrase has >= 3 words)
      - head-trimmed and tail-trimmed partial phrases
      - individually significant single words (len > 3, not a stopword)

    Variants are deduplicated, then sorted so longer/more-specific
    multi-word phrases are prioritized over single words, and finally
    capped at `max_variants` (default 7). Never raises — falls back to
    just the original phrase if `keyword` is empty/whitespace.
    """
    if not keyword or not keyword.strip():
        return []

    original = keyword.strip().lower()
    words = re.findall(r"[a-zA-Z0-9']+", original)
    if not words:
        return [original]

    content_words = [w for w in words if w not in _FUZZY_STOPWORDS]

    variants = set()
    variants.add(original)

    if content_words:
        variants.add(" ".join(content_words))

    # contiguous bigrams
    for i in range(len(words) - 1):
        variants.add(" ".join(words[i:i + 2]))

    # contiguous trigrams
    for i in range(len(words) - 2):
        variants.add(" ".join(words[i:i + 3]))

    # head/tail-trimmed partial phrases
    if len(words) > 1:
        variants.add(" ".join(words[:-1]))
        variants.add(" ".join(words[1:]))

    # individually significant single words
    for w in content_words:
        if len(w) > 3:
            variants.add(w)

    variants.discard("")

    result = list(variants)
    # prioritize longer, multi-word, more-specific phrases first
    result.sort(key=lambda v: (-len(v.split()), -len(v)))

    return result[:max_variants]


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

Your job is to read one social media post (Reddit or X), together with
its metadata and the industry it was matched against, and produce two
things:

1. An intent_score from 1 to 100, built from three weighted components
2. A short, human-written-style reply draft the end user can personalize
   and post themselves, in their own voice, from their own account

You score using the industry context you are given. You are never told
the specific company or product this is for — only the industry
category (e.g. "fintech_payments", "cybersecurity", "crm_sales_tools",
"logistics", "recruitment_hr", "accounting_software"). Two posts using
identical words ("hidden fees are killing us") can score very
differently depending on whether the industry context is fintech
billing versus logistics freight surcharges — use the industry field to
judge whether the post's actual subject matches that vertical's real
buyer pain, not just shared vocabulary.

═══════════════════════════════════════════════════════════════════════
INPUT YOU WILL RECEIVE, PER POST
═══════════════════════════════════════════════════════════════════════
- platform: "reddit" | "x"
- industry: one of the six category strings above
- search_keyword: the phrase this post was matched against
- post_text: the raw post content
- google_rank: integer, or null (X posts will almost always be null —
  see Component 2 below)
- search_volume: monthly search volume for search_keyword, or null
- upvotes / likes: integer, platform-appropriate
- comments: integer

═══════════════════════════════════════════════════════════════════════
SCORING MODEL — 100 POINTS, THREE COMPONENTS
═══════════════════════════════════════════════════════════════════════

── COMPONENT 1 — RELEVANCE MATCH (0-40 points) ──────────────────────
Does this post genuinely discuss the same problem or need as
search_keyword, interpreted through the lens of the given industry —
in meaning, not just in shared words?

  36-40  Unambiguously about exactly this problem, in this industry.
  25-35  Clearly related, but broader, tangential, or partial —
         e.g. discussing the general category without the specific pain.
  10-24  Matching words present, but the actual subject differs, OR the
         pain described belongs to a different industry than the one
         given (e.g. "hidden fees" post is about parking tickets, not
         payment processing).
  0-9    No genuine connection.

THIS COMPONENT IS A HARD GATE.
If relevance scores below 10: is_relevant = false, and intent_score
must not exceed 15 — regardless of how strong Component 2 or 3 look.
A top-ranked, highly-upvoted post about the wrong subject is still a
wrong-subject post.

── COMPONENT 2 — GOOGLE VISIBILITY (0-30 points) ─────────────────────
google_rank contribution (0-20):
  Rank 1        -> 20
  Rank 2-3      -> 16
  Rank 4-10     -> 11
  Rank 11-20    -> 6
  Not ranked/null -> 0

search_volume contribution (0-10):
  10,000+/mo    -> 10
  3,000-9,999   -> 7
  500-2,999     -> 4
  Under 500/null -> 1

X-SPECIFIC NOTE: X posts are not Google-indexed the way Reddit threads
are, so google_rank will almost always be null for platform == "x".
A null rank on an X post is EXPECTED and is not a quality signal one
way or the other — do not treat it as a penalty, and do not attempt to
infer or guess a rank that wasn't provided. Score the 0-point rank
contribution plainly and let Components 1 and 3 carry that post.

── COMPONENT 3 — ENGAGEMENT SIGNAL (0-30 points) ─────────────────────
Derived from upvotes/likes and comments, judged proportionally to
platform norms — the same raw number means different things on
different platforms.

Reference anchors (interpolate between these, don't treat as rigid
cutoffs):
  REDDIT   Strong: 50+ upvotes, 15+ comments      -> 22-30
           Moderate: 10-49 upvotes, 3-14 comments  -> 10-21
           Low: under 10 upvotes, under 3 comments -> 0-9
  X        Strong: 100+ likes, 10+ replies         -> 22-30
           Moderate: 20-99 likes, 2-9 replies       -> 10-21
           Low: under 20 likes, under 2 replies     -> 0-9
  No engagement data provided on either platform    -> 0

FINAL intent_score = Component 1 + Component 2 + Component 3, capped at 100.

═══════════════════════════════════════════════════════════════════════
WORKED EXAMPLES
═══════════════════════════════════════════════════════════════════════

Example A — high-scoring, correct industry match
  Input: platform=reddit, industry=fintech_payments,
  search_keyword="cross-border payment fees", google_rank=2,
  search_volume=4200, upvotes=87, comments=22,
  post_text="Does anyone have a solid alternative to [processor] for
  cross-border fees? We're getting killed on FX markups every month."
  Reasoning: Directly about cross-border payment fees in a fintech
  context (Component 1: 39). Rank 2 + volume 4,200/mo (Component 2:
  16+7=23). 87 upvotes/22 comments on Reddit is strong (Component 3: 26).
  Output: intent_score=88, is_relevant=true,
  reply_draft="Cross-border fees catch a lot of teams off guard —
  worth checking whether your provider discloses FX markup upfront or
  buries it in the settlement rate. Have you compared what you're
  actually losing per transaction?"

Example B — hard-gate failure despite strong surface signals
  Input: platform=reddit, industry=logistics,
  search_keyword="hidden fees", google_rank=1, search_volume=8000,
  upvotes=340, comments=95,
  post_text="Just found out my city adds a hidden fee to every parking
  ticket if you pay online. Total scam."
  Reasoning: Shares the words "hidden fees" but is about parking
  tickets, not logistics/freight pricing (Component 1: 4 — hard gate
  triggered). Rank and engagement are irrelevant once the gate fails.
  Output: intent_score=9, is_relevant=false, reply_draft=null

Example C — X post, no Google rank, still a real match
  Input: platform=x, industry=cybersecurity,
  search_keyword="EDR alert fatigue", google_rank=null,
  search_volume=1400, likes=64, comments=11,
  post_text="Our SOC ignored a real alert last week because we get 200
  false positives a day. Something has to change."
  Reasoning: Directly describes EDR alert fatigue (Component 1: 37).
  google_rank null is expected for X — score 0 for that piece, but
  volume 1,400 still contributes (Component 2: 0+4=4). 64 likes/11
  comments is strong for X (Component 3: 25).
  Output: intent_score=66, is_relevant=true,
  reply_draft="200 false positives a day would burn out any team, not
  just miss the real one. Sounds like the tuning problem is as much
  the issue as the tool itself — has your team looked at what's driving
  the noise ratio that high?"

═══════════════════════════════════════════════════════════════════════
REPLY DRAFT — RULES
═══════════════════════════════════════════════════════════════════════
Only generate reply_draft when is_relevant is true. Otherwise: null.

- Generic and honest — never invent a fake personal story, dollar
  amount, or timeline not present in the input.
- Acknowledge the poster's situation in one clause, then offer one
  genuinely useful angle — not a pitch.
- 2-3 sentences maximum. No links, no "DM me," no product/company name
  (the end user adds that themselves if relevant).
- End on warmth or a question, never a call-to-action.
- AVOID: "I totally understand," "This is so common," or any opener
  that could paste onto literally any post — anchor the first clause
  to a specific detail from post_text so it reads as actually read,
  not templated.

═══════════════════════════════════════════════════════════════════════
OUTPUT FORMAT
═══════════════════════════════════════════════════════════════════════
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
# per-keyword fetch-once-forever cache collection (flintel_keywords) +
# NEW: flintel_google_posts (SERP-discovered post_url cache, decoupled
# from Reddit RSS fetching).
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

        # ── flintel_keywords — FETCH-ONCE-FOREVER cache. UNTOUCHED in v9.12.
        # This collection, its indexes, and every function that reads/writes
        # it (sync_keywords_to_db, get_due_keywords, get_keywords_missing_volume,
        # mark_keyword_fetched, set_keyword_retry_cooldown,
        # seed_search_volume_batch) are byte-for-byte identical to v9.11.1.
        db.flintel_keywords.create_index([("keyword", ASCENDING)], unique=True, name="keyword_unique")
        db.flintel_keywords.create_index([("fetched", ASCENDING)], name="keyword_fetched_idx")
        db.flintel_keywords.create_index([("search_volume", ASCENDING)], name="keyword_volume_idx")
        db.flintel_keywords.create_index([("next_retry_at", ASCENDING)], name="keyword_retry_cooldown_idx")

        # ── flintel_google_posts — NEW in v9.12. Stores every Google-SERP-
        # discovered Reddit post_url the instant SERP discovery finds it —
        # completely independent of whether/when that post's actual Reddit
        # RSS fetch happens. One document per discovered post_url:
        #   post_url        : the exact Reddit post URL Google SERP returned
        #   google_rank      : the real per-post rank from that SERP call
        #   matched_keyword  : the exact Google search keyword that produced it
        #   fuzzy_keywords   : 6-7 auto-generated fuzzy variants of matched_keyword
        #                      (see generate_fuzzy_keywords()) — used for extra
        #                      text-level traceability when RSS-matching runs
        #   subreddit        : subreddit name extracted from post_url
        #   fetched          : False until run_google_posts_rss_matching_loop()
        #                      confirms this exact post_url via subreddit RSS —
        #                      then True, PERMANENTLY (fetch-once-forever, same
        #                      spirit as flintel_keywords)
        #   created_at       : when this document was first saved
        db.flintel_google_posts.create_index(
            [("post_url", ASCENDING)], unique=True, name="google_post_url_unique"
        )
        db.flintel_google_posts.create_index([("fetched", ASCENDING)], name="google_post_fetched_idx")
        db.flintel_google_posts.create_index([("subreddit", ASCENDING)], name="google_post_subreddit_idx")
        db.flintel_google_posts.create_index(
            [("subreddit", ASCENDING), ("fetched", ASCENDING)], name="google_post_subreddit_fetched_idx"
        )

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
# KEYWORD CACHE — flintel_keywords collection. 100% UNCHANGED FROM v9.11.1.
# FETCH-ONCE-FOREVER design: each keyword gets fetched from DataForSEO
# exactly ONE time, ever. Once fetched=True, it is PERMANENTLY skipped by
# get_due_keywords() — no TTL, no re-due date, no 12h/24h re-fetch.
#
# NOTE (v9.12): "fetched=True" here now means "this keyword's Google SERP
# results have all been saved to flintel_google_posts" — it no longer
# means "Reddit RSS was fetched for every result" (that dependency has
# been removed — see process_one_keyword() below). Nothing about the
# flintel_keywords collection itself, its schema, or any function in this
# section changed to make that true; it's a natural consequence of
# process_one_keyword() no longer calling into Reddit's RSS at all.
# ─────────────────────────────────────────────────────────────────────────────

def sync_keywords_to_db(keywords: list):
    """
    Ensures every keyword currently in REDDIT_SEARCH_KEYWORDS exists in
    flintel_keywords. Brand-new keywords are inserted with fetched=False
    and search_volume=None (both due immediately, real-time). Keywords
    that already exist are left completely untouched — $setOnInsert only
    writes on first-ever insert. Safe to call every loop pass and on
    every restart.

    This is INSERT-ONLY and additive — it never deletes or hides a
    keyword's existing document just because that keyword is no longer
    present in `keywords`.
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
                    "next_retry_at":            None,
                    "created_at":               now,
                }},
                upsert=True,
            )
        except Exception as exc:
            log.error(f"[KEYWORD-CACHE] sync error for {kw!r}: {exc}")


def get_keywords_missing_volume(keywords: list = None) -> list:
    """
    Returns keyword strings whose flintel_keywords document has no
    search_volume stored yet (missing field or explicit None both match
    this query). Taken DIRECTLY against the full flintel_keywords
    collection — NOT restricted to "{'keyword': {'$in': keywords}}".
    """
    try:
        cursor = db.flintel_keywords.find(
            {"search_volume": None},
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
    this query. Taken DIRECTLY against the full flintel_keywords
    collection — NOT restricted to the current python list.

    A keyword whose Reddit RSS fetch failed also needs its "next_retry_at"
    cooldown to have passed before it's returned here — see
    REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS and set_keyword_retry_cooldown()
    below. A keyword with next_retry_at unset/None (brand new, never
    attempted) or already in the past is still due immediately.
    """
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


def set_keyword_retry_cooldown(keyword: str, cooldown_seconds: int = REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS):
    """
    Kept 100% as-is from v9.11.2 for API compatibility. As of v9.12,
    process_one_keyword() no longer produces a Reddit-RSS-driven
    had_fetch_failure (that logic moved to the fully separate
    run_google_posts_rss_matching_loop(), which operates on
    flintel_google_posts, not on a per-keyword failure flag) — so this
    function is not currently invoked by the SERP discovery loop, but is
    left untouched in case any future SERP-call-level failure needs the
    same cooldown mechanism.
    """
    now = datetime.now(timezone.utc)
    next_retry = now + timedelta(seconds=cooldown_seconds)
    try:
        db.flintel_keywords.update_one(
            {"keyword": keyword},
            {"$set": {"next_retry_at": next_retry}},
        )
        log.info(
            f"[KEYWORD-CACHE] '{keyword}' cooldown set | next_retry_at:{next_retry.isoformat()} "
            f"({cooldown_seconds}s from now) — will not be re-attempted before then"
        )
    except Exception as exc:
        log.error(f"[KEYWORD-CACHE] set_keyword_retry_cooldown error for {keyword!r}: {exc}")


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
# flintel_google_posts HELPERS (NEW in v9.12)
#
# This collection is the sole source of truth for "which Google-SERP-
# discovered Reddit post_urls are still waiting to be confirmed via
# subreddit RSS?" — completely independent of flintel_keywords, which
# only tracks keyword-level SERP-discovery state.
# ─────────────────────────────────────────────────────────────────────────────

def save_google_post(post_url: str, google_rank, matched_keyword: str, subreddit: str) -> bool:
    """
    Saves ONE newly-discovered Google-SERP result into flintel_google_posts,
    auto-generating its fuzzy_keywords from matched_keyword. Insert-only
    per unique post_url (unique index on post_url) — if this exact
    post_url was already saved in a previous pass, this is a silent no-op
    (duplicate discovery of the same URL, e.g. from a different keyword's
    SERP results overlapping). Does NOT touch flintel_keywords. Does NOT
    wait on or call into any Reddit endpoint — this save is immediate and
    fully independent of Reddit's RSS reliability.
    """
    fuzzy = generate_fuzzy_keywords(matched_keyword, max_variants=FUZZY_KEYWORDS_PER_POST)
    doc = {
        "post_url":        post_url,
        "google_rank":     google_rank,
        "matched_keyword": matched_keyword,
        "fuzzy_keywords":  fuzzy,
        "subreddit":       subreddit,
        "fetched":         False,
        "created_at":      datetime.now(timezone.utc),
    }
    try:
        db.flintel_google_posts.insert_one(doc)
        log.info(
            f"[GOOGLE-POSTS] SAVED | post_url:{post_url} | rank:{google_rank} | "
            f"subreddit:r/{subreddit or '?'} | matched_keyword:{matched_keyword!r} | "
            f"fuzzy_keywords:{fuzzy}"
        )
        return True
    except DuplicateKeyError:
        log.debug(f"[GOOGLE-POSTS] Duplicate post_url skipped (already cached): {post_url}")
        return False
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] save_google_post error for {post_url}: {exc}")
        return False


def get_pending_google_post_subreddits() -> list:
    """
    Returns the list of DISTINCT subreddit names that currently have at
    least one fetched=False document in flintel_google_posts. This is
    read DIRECTLY off the collection every single pass — no python list
    of subreddits is ever maintained separately.
    """
    try:
        subs = db.flintel_google_posts.distinct("subreddit", {"fetched": False})
        return [s for s in subs if s]
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] get_pending_google_post_subreddits error: {exc}")
        return []


def get_pending_google_posts_for_subreddit(subreddit: str) -> list:
    """
    Returns every fetched=False flintel_google_posts document for one
    subreddit — the exact set of post_urls run_google_posts_rss_matching_loop()
    is currently trying to confirm via that subreddit's RSS feed.
    """
    try:
        return list(db.flintel_google_posts.find({"subreddit": subreddit, "fetched": False}))
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] get_pending_google_posts_for_subreddit error for r/{subreddit}: {exc}")
        return []


def mark_google_post_fetched(post_url: str):
    """
    Flips a flintel_google_posts document to fetched=True — PERMANENTLY,
    same fetch-once-forever spirit as mark_keyword_fetched() above. Once
    true, this post_url will never be returned by
    get_pending_google_posts_for_subreddit() again.
    """
    now = datetime.now(timezone.utc)
    try:
        db.flintel_google_posts.update_one(
            {"post_url": post_url},
            {"$set": {"fetched": True, "fetched_at": now}},
        )
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] mark_google_post_fetched error for {post_url}: {exc}")


def get_cached_search_volume_for_keyword(keyword: str) -> tuple:
    """
    Read-only lookup straight off flintel_keywords for a single keyword's
    already-seeded search_volume + search_volume_is_random flag. NEVER
    triggers a new API call, NEVER writes to flintel_keywords — this is
    purely a cache read used by run_google_posts_rss_matching_loop() so
    that stage never re-queries the search-volume API itself.
    Returns (search_volume_or_None, is_random_bool).
    """
    try:
        doc = db.flintel_keywords.find_one(
            {"keyword": keyword}, {"search_volume": 1, "search_volume_is_random": 1}
        )
        if not doc:
            return None, False
        return doc.get("search_volume"), bool(doc.get("search_volume_is_random", False))
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] get_cached_search_volume_for_keyword error for {keyword!r}: {exc}")
        return None, False


# ─────────────────────────────────────────────────────────────────────────────
# SEARCH-VOLUME BATCH SEEDING — 100% UNCHANGED FROM v9.11.1. chunks
# keywords, fetches volume for each one (single.php only accepts one
# keyword per call), writes results back onto each keyword's own
# flintel_keywords document.
# ─────────────────────────────────────────────────────────────────────────────

def seed_search_volume_batch(keywords_needing_volume: list, batch_size: int = SEARCH_VOLUME_BATCH_SIZE):
    """
    ONE-TIME (per keyword) BATCH search-volume seeding. Splits
    `keywords_needing_volume` into chunks of up to `batch_size` and
    fetches volume for every keyword in the chunk. Results are written
    back onto each keyword's own flintel_keywords document
    (search_volume field, plus search_volume_is_random).
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
# 100% UNCHANGED FROM v9.11.1.
# ─────────────────────────────────────────────────────────────────────────────

def fetch_search_volume(search_keyword: str) -> int | None:
    """
    Monthly search volume — a SINGLE keyword, single request. Kept for
    the Twitter fallback path (fetch_google_stats(), used only when
    SEARCH_KEYWORD is configured for Twitter items).
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
    Twitter items. 100% UNCHANGED FROM v9.11.1.
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
# (site:reddit.com) -> real per-post rank + URL -> flintel_google_posts
# cache (v9.12). search_google_for_keyword() itself is 100% UNCHANGED
# FROM v9.11.1 — it still runs unconditionally whenever a keyword is due,
# on its own dedicated RapidAPI host, completely independent of the
# search-volume host/call, and completely independent of Reddit's RSS
# reliability (which now lives entirely in
# run_google_posts_rss_matching_loop() below).
# ─────────────────────────────────────────────────────────────────────────────

def search_google_for_keyword(keyword: str, months_back: int = SERP_MONTHS_BACK) -> list:
    """
    RapidAPI Google search restricted to site:reddit.com, rolling
    last-N-months date window. Returns real per-result rank + URL. Only
    called for keywords that get_due_keywords() has flagged as due.
    100% UNCHANGED FROM v9.11.1.
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
    Reddit fetch or Claude scoring happens. 100% UNCHANGED FROM v9.11.1.
    """
    if not post_url:
        return False
    try:
        existing = db.signals.find_one({"post_url": post_url}, {"_id": 1})
        return existing is not None
    except Exception as exc:
        log.error(f"[DEDUP] is_post_already_signaled error for {post_url}: {exc}")
        return False   # fail-open: if the check itself fails, don't block discovery


def _extract_reddit_subreddit_from_url(post_url: str) -> str:
    """Pulls the subreddit name out of a standard reddit.com post URL
    (e.g. reddit.com/r/<subreddit>/comments/...). Returns "" if it
    can't be found — never raises."""
    match = re.search(r"reddit\.com/r/([^/]+)/", post_url)
    return match.group(1) if match else ""


def _extract_reddit_submission_id(post_url: str) -> str | None:
    """Pulls the submission id out of a standard reddit.com post URL
    (e.g. .../comments/<id>/...). Used to build a stable message_id."""
    match = re.search(r"/comments/([a-zA-Z0-9]+)", post_url)
    return match.group(1) if match else None


def _normalize_reddit_url(url: str) -> str:
    """Normalizes a Reddit post URL for exact-match comparison between a
    SERP-discovered post_url and an RSS entry's link: strips query
    string/fragment, trailing slash, and the www./old. host prefixes."""
    if not url:
        return ""
    url = url.split("?")[0].split("#")[0].rstrip("/")
    url = url.replace("https://old.reddit.com", "https://www.reddit.com")
    url = url.replace("https://reddit.com", "https://www.reddit.com")
    return url.lower()


def process_one_keyword(keyword: str) -> tuple:
    """
    v9.12: SERP-DISCOVERY-ONLY. Full discovery work for ONE keyword that
    get_due_keywords() has flagged as due right now:
      1. RapidAPI SERP search (site:reddit.com, last N months) — 100%
         unchanged call (search_google_for_keyword()).
      2. Per-result post_url dedup check -> skip posts already scored
         (in `signals`) or already cached (in flintel_google_posts).
      3. For every genuinely new result: extract the subreddit, and save
         {post_url, google_rank, matched_keyword, fuzzy_keywords,
         subreddit, fetched:False} into flintel_google_posts via
         save_google_post(). This save is immediate — it never calls
         into Reddit's RSS/JSON endpoints and never waits on them.

    Returns (new_items_count, skipped_dupes_count, had_fetch_failure) for
    logging AND for run_serp_discovery_loop()'s fetched=True decision.
    had_fetch_failure is now ALWAYS False here — v9.12 fully decouples
    Reddit RSS fetching from SERP discovery, so a keyword's SERP results
    being saved to flintel_google_posts can never fail due to Reddit's
    RSS reliability. (The tuple shape is kept unchanged so
    run_serp_discovery_loop()'s existing branching logic below doesn't
    need restructuring.)
    """
    new_items, skipped_dupes = 0, 0
    had_fetch_failure = False  # v9.12: Reddit RSS fetch failures can no longer occur at this stage

    results = search_google_for_keyword(keyword, months_back=SERP_MONTHS_BACK)

    for result in results:
        post_url = result["url"]

        if is_post_already_signaled(post_url):
            skipped_dupes += 1
            log.debug(f"[SERP] Skipping already-signaled post_url: {post_url}")
            continue

        subreddit = _extract_reddit_subreddit_from_url(post_url)
        saved = save_google_post(
            post_url=post_url,
            google_rank=result["rank"],
            matched_keyword=keyword,
            subreddit=subreddit,
        )
        if saved:
            new_items += 1
        else:
            skipped_dupes += 1
        time.sleep(0.05)  # tiny pacing between DB writes only — no external call here

    return new_items, skipped_dupes, had_fetch_failure


def run_serp_discovery_loop():
    """
    Continuously polls flintel_keywords every KEYWORD_CHECK_INTERVAL_SECONDS
    for keywords that have NEVER been fetched (fetched=False), and for any
    keyword still missing a cached search_volume (batch-seeds it). 100%
    UNCHANGED FROM v9.11.1 in its keyword-cache behavior — the only
    difference is what process_one_keyword() does per due keyword (see
    that function's docstring): it now saves discovered post_urls into
    flintel_google_posts instead of fetching each one's Reddit RSS
    directly, so a keyword's fetched=True marking here depends only on
    SERP discovery finishing, never on Reddit's RSS reliability.
    """
    sync_keywords_to_db(REDDIT_SEARCH_KEYWORDS)

    missing_volume = get_keywords_missing_volume()
    if missing_volume:
        log.info(
            f"[VOLUME-SEED] {len(missing_volume)} keyword(s) need search_volume — "
            f"seeding in batches of {SEARCH_VOLUME_BATCH_SIZE}..."
        )
        seed_search_volume_batch(missing_volume, batch_size=SEARCH_VOLUME_BATCH_SIZE)

    log.info(
        f"[SERP] Discovery loop started | {len(REDDIT_SEARCH_KEYWORDS)} keyword(s) in python list | "
        f"check_interval:{KEYWORD_CHECK_INTERVAL_SECONDS}s | "
        f"months_back:{SERP_MONTHS_BACK} | depth:{SERP_RESULTS_PER_KEYWORD} | "
        f"KEYWORD CACHE: fetch-once-forever, restart-safe, no re-fetch ever, "
        f"due/missing-volume read from flintel_keywords directly (not filtered by python list) | "
        f"SEARCH-VOLUME: batched loop (size {SEARCH_VOLUME_BATCH_SIZE}) | "
        f"random fallback range {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-{SEARCH_VOLUME_RANDOM_FALLBACK_MAX} "
        f"on failure/no-credits (always logged) | "
        f"v9.12: SERP results now saved into flintel_google_posts immediately — "
        f"Reddit RSS fetching is fully decoupled (see run_google_posts_rss_matching_loop)"
    )

    while True:
        try:
            sync_keywords_to_db(REDDIT_SEARCH_KEYWORDS)

            missing_volume = get_keywords_missing_volume()
            if missing_volume:
                seed_search_volume_batch(missing_volume, batch_size=SEARCH_VOLUME_BATCH_SIZE)

            due = get_due_keywords()
            if not due:
                time.sleep(KEYWORD_CHECK_INTERVAL_SECONDS)
                continue

            total_new, total_dupes = 0, 0
            for doc in due:
                keyword = doc["keyword"]
                new_items, dupes, had_fetch_failure = process_one_keyword(keyword)
                total_new += new_items
                total_dupes += dupes

                # v9.12: had_fetch_failure is always False now (see
                # process_one_keyword docstring) — a keyword is always
                # marked fetched=True once its SERP results are saved to
                # flintel_google_posts, since that save no longer depends
                # on Reddit's RSS reliability at all.
                mark_keyword_fetched(keyword)
                log.info(
                    f"[SERP] '{keyword}' DONE | new_google_posts:{new_items} skipped_dupes:{dupes} | "
                    f"marked fetched=True PERMANENTLY — will never be re-fetched | "
                    f"Reddit RSS confirmation for these post_urls will happen independently "
                    f"via run_google_posts_rss_matching_loop()"
                )
                time.sleep(SERP_FETCH_SLEEP_SECONDS)

            log.info(
                f"[SERP] Pass complete | keywords_processed:{len(due)} | "
                f"new_google_posts:{total_new} | skipped_dupes:{total_dupes}"
            )

        except Exception as exc:
            log.error(f"[SERP] discovery loop error: {exc}")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT SUBREDDIT-RSS FETCH — public, credential-free /r/<subreddit>/new.rss
# feed. Smart-retry logic (v9.6) unchanged in spirit — same User-Agent,
# jittered backoff, old.reddit.com fallback host — just applied to a
# subreddit's feed URL instead of a single post's .rss URL, since v9.12
# no longer fetches one post_url at a time.
# ─────────────────────────────────────────────────────────────────────────────

def _reddit_get_with_retry(url: str) -> requests.Response | None:
    """
    "Smart" GET wrapper for Reddit's public endpoints — retry/backoff/
    jitter behavior kept exactly as prior versions:
      - Reddit-recommended User-Agent format (REDDIT_USER_AGENT).
      - Small randomized jitter delay before each attempt.
      - Exponential backoff retry, up to REDDIT_FETCH_MAX_RETRIES times,
        specifically for 403 / 429 / 5xx responses.
    Returns the Response on success (status 200), or None if every
    attempt failed.
    """
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
                log.debug(f"[REDDIT-RSS] 404 (gone) for {url} — not retrying.")
                return None
            if r.status_code in (403, 429) or r.status_code >= 500:
                wait = (REDDIT_FETCH_BACKOFF_BASE ** attempt) + random.uniform(0, 1.0)
                log.warning(
                    f"[REDDIT-RSS] fetch attempt {attempt}/{REDDIT_FETCH_MAX_RETRIES} "
                    f"got {r.status_code} for {url} — backing off {wait:.1f}s..."
                )
                time.sleep(wait)
                continue
            log.error(f"[REDDIT-RSS] Unexpected status {r.status_code} for {url}")
            return None
        except requests.RequestException as exc:
            log.warning(
                f"[REDDIT-RSS] fetch attempt {attempt}/{REDDIT_FETCH_MAX_RETRIES} "
                f"network error for {url}: {exc}"
            )
            time.sleep((REDDIT_FETCH_BACKOFF_BASE ** attempt))

    log.error(f"[REDDIT-RSS] fetch exhausted {REDDIT_FETCH_MAX_RETRIES} attempts for {url} "
              f"(last_status:{last_status})")
    return None


def _fetch_subreddit_rss(subreddit: str) -> list:
    """
    Fetches r/<subreddit>/new.rss (public, credential-free), with
    old.reddit.com fallback host on failure — same smart-retry as the
    prior per-post fetch, just pointed at a subreddit feed instead.
    Returns a list of parsed feedparser entries (possibly empty).
    """
    primary_url = f"https://www.reddit.com/r/{subreddit}/new.rss"
    r = _reddit_get_with_retry(primary_url)

    if r is None:
        fallback_url = f"https://old.reddit.com/r/{subreddit}/new.rss"
        log.info(f"[REDDIT-RSS] Retrying r/{subreddit} via old.reddit.com fallback...")
        r = _reddit_get_with_retry(fallback_url)

    if r is None:
        log.error(f"[REDDIT-RSS] Giving up on r/{subreddit} this pass — will retry next cycle.")
        return []

    try:
        feed = feedparser.parse(r.content)
        return feed.entries[:GOOGLE_POSTS_RSS_ENTRY_LIMIT]
    except Exception as exc:
        log.error(f"[REDDIT-RSS] parse error for r/{subreddit}: {exc}")
        return []


def _entry_to_text_and_meta(entry) -> dict:
    """
    Extracts text/username/posted_at from one feedparser RSS entry —
    same extraction logic used by the prior per-post RSS fetch.
    """
    title = (entry.get("title", "") or "").strip()
    raw_summary = entry.get("summary", "") or ""
    if not raw_summary and entry.get("content"):
        raw_summary = entry["content"][0].get("value", "") or ""
    summary_plain = re.sub(r"<[^>]+>", " ", html.unescape(raw_summary)).strip()

    text = title
    if summary_plain and summary_plain.lower() != title.lower():
        text = f"{title}\n\n{summary_plain}"

    author = (entry.get("author", "") or "unknown").lstrip("u/").lstrip("/u/").strip() or "unknown"

    posted_at = None
    published = entry.get("published") or entry.get("updated")
    if published:
        try:
            posted_at = datetime(*entry.get("published_parsed", entry.get("updated_parsed"))[:6],
                                  tzinfo=timezone.utc).isoformat()
        except (TypeError, ValueError):
            posted_at = published

    link = entry.get("link", "") or ""

    return {"text": text, "author": author, "posted_at": posted_at, "link": link}


def run_google_posts_rss_matching_loop():
    """
    NEW in v9.12 — the ONLY place in this system that talks to Reddit's
    RSS feeds now. Fully independent of, and never blocks or is blocked
    by, run_serp_discovery_loop() / process_one_keyword() / flintel_keywords.

    Every GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS:
      1. Reads flintel_google_posts DIRECTLY for the distinct list of
         subreddits that still have at least one fetched=False document
         (get_pending_google_post_subreddits()) — no python list of
         subreddits/keywords/fuzzy-keywords is ever maintained separately;
         this collection is the sole source of truth, read fresh every
         pass, exactly like flintel_keywords is for SERP discovery.
      2. For each such subreddit, fetches that subreddit's public,
         credential-free /new.rss feed (smart-retry + old.reddit.com
         fallback — same logic as before, just pointed at the subreddit
         instead of a single post).
      3. Builds a lookup of this subreddit's still-pending
         flintel_google_posts documents keyed by NORMALIZED post_url.
      4. For every RSS entry returned, normalizes its link and checks it
         against that lookup. A match on post_url is the AUTHORITATIVE
         signal — this is the exact post Google's SERP already told us
         about, at a known rank, for a known keyword. The document's
         stored fuzzy_keywords are cross-checked against the entry's text
         PURELY for extra traceability in the log line below (never
         blocking — an exact post_url match is already fully sufficient
         confirmation).
      5. On a match:
           - pulls that keyword's already-seeded search_volume straight
             off flintel_keywords via get_cached_search_volume_for_keyword()
             (read-only — NEVER calls the search-volume API itself)
           - generates the random engagement fallback (RSS has no real
             upvotes/comments, same as before)
           - builds the exact same item schema as the old per-post fetch
             produced, pushes it into reddit_queue + save_queue_message()
           - marks that flintel_google_posts document fetched=True,
             permanently (mark_google_post_fetched())
      6. Any RSS entry that does NOT match a pending post_url for that
         subreddit is simply ignored here (it wasn't a Google-SERP-
         discovered post we're tracking) — no separate keyword filtering
         against a python list happens at this stage; the authoritative
         gate is always "is this post_url one flintel_google_posts is
         waiting to confirm?".
    """
    log.info(
        f"[GOOGLE-POSTS-RSS] Matching loop started | check_interval:"
        f"{GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS}s | rss_entry_limit:"
        f"{GOOGLE_POSTS_RSS_ENTRY_LIMIT} | reads flintel_google_posts directly, "
        f"no python list of subreddits ever maintained"
    )

    while True:
        try:
            subreddits = get_pending_google_post_subreddits()
            if not subreddits:
                log.debug("[GOOGLE-POSTS-RSS] No pending subreddits this pass — sleeping.")
                time.sleep(GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS)
                continue

            log.info(
                f"[GOOGLE-POSTS-RSS] Pass starting | {len(subreddits)} subreddit(s) "
                f"with pending (fetched=False) post_url(s): {subreddits}"
            )

            total_confirmed, total_subreddits_processed = 0, 0

            for subreddit in subreddits:
                pending_docs = get_pending_google_posts_for_subreddit(subreddit)
                if not pending_docs:
                    continue

                pending_by_url = {_normalize_reddit_url(d["post_url"]): d for d in pending_docs}

                log.info(
                    f"[GOOGLE-POSTS-RSS] r/{subreddit} | polling RSS | "
                    f"{len(pending_by_url)} pending post_url(s) to confirm"
                )

                entries = _fetch_subreddit_rss(subreddit)
                total_subreddits_processed += 1

                if not entries:
                    log.warning(f"[GOOGLE-POSTS-RSS] r/{subreddit} | RSS returned no entries this pass.")
                    time.sleep(SERP_FETCH_SLEEP_SECONDS)
                    continue

                confirmed_this_subreddit = 0

                for entry in entries:
                    meta = _entry_to_text_and_meta(entry)
                    normalized_link = _normalize_reddit_url(meta["link"])
                    if not normalized_link or normalized_link not in pending_by_url:
                        continue

                    doc = pending_by_url[normalized_link]
                    post_url = doc["post_url"]
                    matched_keyword = doc.get("matched_keyword", SEARCH_KEYWORD)
                    fuzzy_keywords = doc.get("fuzzy_keywords", [])
                    google_rank = doc.get("google_rank")

                    fuzzy_hit = any(fk.lower() in meta["text"].lower() for fk in fuzzy_keywords) if fuzzy_keywords else False

                    search_volume, sv_is_random = get_cached_search_volume_for_keyword(matched_keyword)

                    upvotes = _random_engagement_fallback()
                    comments = _random_engagement_fallback()

                    submission_id = _extract_reddit_submission_id(post_url)
                    message_id = f"reddit_serp_{submission_id}" if submission_id else (
                        f"reddit_serp_{re.sub(r'[^a-zA-Z0-9]', '_', post_url)[-40:]}"
                    )

                    item = {
                        "message_id":              message_id,
                        "platform":                "reddit",
                        "text":                    meta["text"],
                        "username":                meta["author"],
                        "subreddit_or_channel":    subreddit,
                        "post_url":                post_url,
                        "posted_at":               meta["posted_at"],
                        "search_keyword":          matched_keyword,
                        "upvotes":                 upvotes,
                        "comments":                comments,
                        "engagement_is_random":    True,
                        "google_rank":             google_rank,
                        "search_volume":           search_volume,
                        "search_volume_is_random": sv_is_random,
                    }

                    reddit_queue.put(item)
                    save_queue_message("reddit", item)
                    mark_google_post_fetched(post_url)

                    confirmed_this_subreddit += 1
                    total_confirmed += 1

                    sv_tag = "RANDOM-FALLBACK" if sv_is_random else "real"
                    log.info(
                        f"[GOOGLE-POSTS-RSS] CONFIRMED via URL match | r/{subreddit} | "
                        f"post_url:{post_url} | google_rank:{google_rank} | "
                        f"matched_keyword:{matched_keyword!r} | fuzzy_keyword_text_hit:{fuzzy_hit} | "
                        f"search_volume:{search_volume} ({sv_tag}, from flintel_keywords cache) | "
                        f"upvotes:{upvotes} comments:{comments} (RANDOM-FALLBACK, RSS has no real counts) | "
                        f"queued for Claude scoring | marked fetched=True PERMANENTLY in flintel_google_posts"
                    )

                if confirmed_this_subreddit == 0:
                    log.info(
                        f"[GOOGLE-POSTS-RSS] r/{subreddit} | {len(entries)} RSS entr(y/ies) checked | "
                        f"0 matched a pending post_url this pass — will retry next cycle"
                    )
                else:
                    log.info(
                        f"[GOOGLE-POSTS-RSS] r/{subreddit} | {confirmed_this_subreddit} post_url(s) "
                        f"confirmed and queued this pass"
                    )

                time.sleep(SERP_FETCH_SLEEP_SECONDS)

            log.info(
                f"[GOOGLE-POSTS-RSS] Pass complete | subreddits_processed:{total_subreddits_processed} | "
                f"total_confirmed_and_queued:{total_confirmed}"
            )

        except Exception as exc:
            log.error(f"[GOOGLE-POSTS-RSS] matching loop error: {exc}")
            time.sleep(10)

        time.sleep(GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS)


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE BATCH SCORER — streaming transport + partial-JSON recovery.
# 100% UNCHANGED FROM v9.11.1.
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
        model="claude-haiku-4-5-20251001",
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
# MONGODB STORAGE — 100% UNCHANGED FROM v9.11.1.
# ─────────────────────────────────────────────────────────────────────────────

def save_new_signal(item: dict, score_result: dict, force_pending: bool = False) -> bool:
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
        log.info(
            f"SAVED [{doc['platform'].upper()}] {doc['search_keyword']!r} | "
            f"search_volume:{doc['search_volume']}/mo ({sv_tag}) | "
            f"upvotes:{doc['upvotes']} comments:{doc['comments']} ({eng_tag}) | "
            f"google_rank:{doc['google_rank']} | "
            f"post_url:{doc['post_url']}"
        )
        return True
    except DuplicateKeyError:
        return False
    except Exception as exc:
        log.error(f"MongoDB save error: {exc}")
        log_operator_alert("MongoDB Write Failed", str(exc), level="CRITICAL")
        return False


def replace_confirmed_signal(message_id: str, enrichment: dict, score_result: dict) -> bool:
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
# 100% UNCHANGED FROM v9.11.1.
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
# RESCORE PROCESSOR — 100% UNCHANGED FROM v9.11.1.
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
                replace_confirmed_signal(item["message_id"], enrichment, sr)

            log.info(f"[RESCORE] BATCH {total_batches} DONE — waiting {RESCORE_BATCH_GAP_SECONDS}s...")
            time.sleep(RESCORE_BATCH_GAP_SECONDS)

        except Exception as exc:
            log.error(f"[RESCORE] processor error: {exc}")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER / X POLLER — 100% UNCHANGED FROM v9.11.1.
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
    v9.12: Reddit now runs THREE independent threads instead of two:
      1. SERP discovery thread (run_serp_discovery_loop) — unchanged
         keyword-cache behavior, now saves into flintel_google_posts
         instead of fetching Reddit RSS directly.
      2. flintel_google_posts RSS-matching thread
         (run_google_posts_rss_matching_loop) — NEW — the only thread
         that talks to Reddit's RSS feeds; fully independent of #1.
      3. Its dedicated batch processor thread (unchanged).
    Governed entirely by REDDIT_ENABLED + RapidAPI credentials (RapidAPI
    is required for SERP discovery; the subreddit RSS fetch step needs no
    credentials at all — no OAuth/PRAW).
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
    google_posts_thread = threading.Thread(
        target=run_google_posts_rss_matching_loop, daemon=True, name="Reddit-GooglePosts-RSS"
    )
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT", REDDIT_BATCH_GAP_SECONDS,
              REDDIT_BATCH_TIMEOUT_SECONDS, REDDIT_SEARCH_KEYWORDS),
        daemon=True, name="Reddit-Batch",
    )
    serp_thread.start()
    google_posts_thread.start()
    btch_thread.start()
    log.info(
        f"Reddit threads running: SERP-Discovery ✅ | GooglePosts-RSS-Matching ✅ | Batch ✅ | "
        f"gap:{REDDIT_BATCH_GAP_SECONDS}s | timeout:{REDDIT_BATCH_TIMEOUT_SECONDS}s"
    )

    while True:
        await asyncio.sleep(60)
        if not serp_thread.is_alive():
            log.error("Reddit SERP thread died — restarting...")
            serp_thread = threading.Thread(target=run_serp_discovery_loop, daemon=True, name="Reddit-SERP")
            serp_thread.start()
        if not google_posts_thread.is_alive():
            log.error("Reddit GooglePosts-RSS-Matching thread died — restarting...")
            google_posts_thread = threading.Thread(
                target=run_google_posts_rss_matching_loop, daemon=True, name="Reddit-GooglePosts-RSS"
            )
            google_posts_thread.start()
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
    title="Flintel v9.12 — Reddit (SERP + fetch-once-forever keyword cache + flintel_google_posts cache + fuzzy-keyword/URL-match subreddit RSS confirmation + random-fallback volume/engagement) + Twitter Signal Scorer",
    description=(
        "Reddit (RapidAPI SERP discovery, fetch-once-forever keyword cache — "
        "no re-fetch, ever, once a keyword's SERP results are cached) + "
        "Twitter signals: monitor, score (generic 1-100 relevance/visibility/"
        "engagement model), store. v9.12: Reddit's per-post RSS fetch is fully "
        "decoupled from SERP discovery via a new flintel_google_posts "
        "collection — every SERP-discovered post_url + google_rank + matched "
        "keyword + 6-7 auto-generated fuzzy keyword variants + subreddit is "
        "cached immediately (no wait on Reddit at all), and a fully separate "
        "background thread reads that collection directly (never a python "
        "list) to poll each pending subreddit's public /new.rss feed, "
        "confirming posts by exact post_url match and pulling the matched "
        "keyword's search_volume straight from the flintel_keywords cache "
        "(read-only, never a new API call). flintel_keywords, the SERP call, "
        "and the search-volume seeding logic are all 100% unchanged from "
        "v9.11.1. Persistent batch state + queue + dedup — no in-flight item "
        "is ever lost on restart. Streaming Claude with partial-JSON "
        "recovery. Claude failures route to status='pending' for automatic "
        "rescore."
    ),
    version="9.12.0",
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
    missing_volume_count = db.flintel_keywords.count_documents({"search_volume": None})
    random_volume_count = db.flintel_keywords.count_documents({"search_volume_is_random": True})

    total_google_posts = db.flintel_google_posts.count_documents({})
    pending_google_posts = db.flintel_google_posts.count_documents({"fetched": False})
    confirmed_google_posts = db.flintel_google_posts.count_documents({"fetched": True})
    pending_subreddits = db.flintel_google_posts.distinct("subreddit", {"fetched": False})

    return {
        "status":                  "running",
        "system":                  "FLINTEL v9.12.0 (Reddit SERP + fetch-once-forever keyword cache + flintel_google_posts cache + fuzzy-keyword/URL-match RSS confirmation + random-fallback volume/engagement + Twitter)",
        "client":                  CLIENT_ID,
        "platforms":               ["reddit", "twitter"],
        "reddit_enabled":          REDDIT_ENABLED,
        "reddit_status":           _working(REDDIT_ENABLED and bool(RAPIDAPI_KEY)),
        "reddit_fetch_method":     "SERP discovery (RapidAPI) -> flintel_google_posts cache -> subreddit RSS confirmation (credential-free, smart-retry + old.reddit.com fallback) — no OAuth/PRAW",
        "twitter_enabled":         TWITTER_ENABLED,
        "twitter_status":          _working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN)),
        "reddit_search_keywords":  len(REDDIT_SEARCH_KEYWORDS),
        "twitter_search_keywords": len(TWITTER_SEARCH_KEYWORDS),
        "keyword_check_interval_seconds": KEYWORD_CHECK_INTERVAL_SECONDS,
        "keyword_cache":                  "ENABLED — fetch-once-forever, restart-safe (flintel_keywords), UNCHANGED from v9.11.1, no longer tied to Reddit RSS reliability",
        "search_volume_seeding":           f"BATCHED loop (chunks of {SEARCH_VOLUME_BATCH_SIZE}) — UNCHANGED",
        "search_volume_random_fallback":   f"ENABLED — range {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-{SEARCH_VOLUME_RANDOM_FALLBACK_MAX}, always logged, never overrides a real value",
        "google_posts_cache":              "ENABLED (NEW) — flintel_google_posts, fetch-once-forever per post_url, read directly (no python list)",
        "google_posts_rss_check_interval_seconds": GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS,
        "fuzzy_keywords_per_post":         FUZZY_KEYWORDS_PER_POST,
        "reddit_engagement_random_fallback": f"ENABLED — range {REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN}-{REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX} (RSS has no real upvotes/comments), always logged",
        "keywords_tracked":               total_keywords_tracked,
        "keywords_due_now":               due_now_count,
        "keywords_missing_search_volume": missing_volume_count,
        "keywords_with_random_search_volume": random_volume_count,
        "google_posts_total":             total_google_posts,
        "google_posts_pending_rss_confirmation": pending_google_posts,
        "google_posts_confirmed":          confirmed_google_posts,
        "google_posts_pending_subreddits": pending_subreddits,
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
        "reddit_per_post_json_removed": True,
        "reddit_oauth_praw_removed": True,
        "fixed_full_cycle_sleep_removed": True,
        "post_url_dedup_before_scoring": True,
        "claude_failure_routes_to_pending": True,
        "keyword_due_state_independent_of_python_list": True,
        "google_posts_state_independent_of_python_list": True,
        "reddit_serp_never_waits_on_reddit_rss": True,
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
        "reddit_fetch_method":     "SERP -> flintel_google_posts -> subreddit RSS confirmation (credential-free) — no OAuth/PRAW",
        "twitter_working":         TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN),
        "twitter_indicator":       _working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN)),
        "reddit_queue_size":       reddit_queue.qsize(),
        "twitter_queue_size":      twitter_queue.qsize(),
        "google_posts_pending":    db.flintel_google_posts.count_documents({"fetched": False}),
        "rescore_pending":         db.signals.count_documents({"status": "pending"}),
        "client_id":               CLIENT_ID,
        "timestamp":               datetime.now(timezone.utc).isoformat(),
    }


@app.get("/keywords", dependencies=[Depends(verify_api_key)])
def get_keywords_status():
    """
    Inspect the fetch-once-forever keyword cache directly — UNCHANGED
    FROM v9.11.1. Note: "fetched=True" here now means "this keyword's
    SERP results are all cached in flintel_google_posts" (see
    process_one_keyword() docstring) — actual Reddit RSS confirmation
    status lives in /google-posts below.
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


@app.get("/google-posts", dependencies=[Depends(verify_api_key)])
def get_google_posts_status(subreddit: str = None, pending_only: bool = False, limit: int = 200):
    """
    NEW in v9.12 — inspect the flintel_google_posts cache directly. Shows
    every SERP-discovered post_url, its google_rank, matched_keyword,
    auto-generated fuzzy_keywords, subreddit, and whether it has been
    confirmed yet (fetched=True) via run_google_posts_rss_matching_loop().
    """
    q: dict = {}
    if subreddit:
        q["subreddit"] = subreddit
    if pending_only:
        q["fetched"] = False

    raw_docs = list(db.flintel_google_posts.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
    docs = []
    for d in raw_docs:
        for f in ["created_at", "fetched_at"]:
            if d.get(f):
                d[f] = d[f].isoformat()
        docs.append(d)

    total = db.flintel_google_posts.count_documents({})
    pending = db.flintel_google_posts.count_documents({"fetched": False})
    confirmed = db.flintel_google_posts.count_documents({"fetched": True})
    pending_subreddits = db.flintel_google_posts.distinct("subreddit", {"fetched": False})

    return {
        "total": total,
        "pending": pending,
        "confirmed": confirmed,
        "pending_subreddits": pending_subreddits,
        "count_returned": len(docs),
        "google_posts": docs,
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
    log.info("  FLINTEL v9.12.0 — REDDIT (SERP + FETCH-ONCE-FOREVER KEYWORD CACHE")
    log.info("                   + FLINTEL_GOOGLE_POSTS CACHE + FUZZY-KEYWORD/URL-MATCH")
    log.info("                   SUBREDDIT RSS CONFIRMATION + RANDOM-FALLBACK VOLUME/")
    log.info("                   ENGAGEMENT) + TWITTER SIGNAL SCORER")
    log.info("=" * 70)
    log.info(f"  Client               : {CLIENT_ID}")
    log.info(f"  Platforms            : Reddit (SERP discovery, fetch-once-forever) + Twitter/X")
    log.info(f"  Reddit               : {REDDIT_ENABLED} | {_working(REDDIT_ENABLED and bool(RAPIDAPI_KEY))}")
    log.info(f"  Reddit fetch method  : SERP (RapidAPI) -> flintel_google_posts cache -> subreddit RSS "
             f"confirmation — credential-free, no OAuth/PRAW, nothing to configure")
    log.info(f"  Reddit engagement    : RANDOM placeholder {REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN}-{REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX} (upvotes/comments) — RSS has no real counts, always logged")
    log.info(f"  Twitter              : {TWITTER_ENABLED} | {_working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN))}")
    log.info(f"  Reddit keywords      : {len(REDDIT_SEARCH_KEYWORDS)} (used for SERP discovery + to seed brand-new flintel_keywords docs)")
    log.info(f"  Twitter keywords     : {len(TWITTER_SEARCH_KEYWORDS)} (used for Twitter search query)")
    log.info(f"  Keyword cache        : fetch-once-forever (no re-fetch, ever) | check every {KEYWORD_CHECK_INTERVAL_SECONDS}s | "
             f"last {SERP_MONTHS_BACK} months | depth {SERP_RESULTS_PER_KEYWORD} | UNCHANGED from v9.11.1")
    log.info(f"  Keyword due state    : read directly from flintel_keywords — NOT filtered by the current "
             f"REDDIT_SEARCH_KEYWORDS python list")
    log.info(f"  Search-volume seeding: batched loop, chunks of {SEARCH_VOLUME_BATCH_SIZE} keywords | UNCHANGED from v9.11.1")
    log.info(f"  Search-volume fallback: RANDOM placeholder {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-"
             f"{SEARCH_VOLUME_RANDOM_FALLBACK_MAX} on any failure/no-credits — always clearly logged")
    log.info(f"  flintel_google_posts : NEW — every SERP-discovered post_url + google_rank + matched_keyword "
             f"+ {FUZZY_KEYWORDS_PER_POST} auto fuzzy keywords + subreddit cached immediately, no wait on Reddit")
    log.info(f"  Google-posts RSS     : independent thread | check every {GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS}s | "
             f"reads flintel_google_posts directly (no python list) | confirms by exact post_url match | "
             f"{REDDIT_FETCH_MAX_RETRIES}x backoff + old.reddit.com fallback")
    log.info(f"  Reddit batch         : {REDDIT_BATCH_SIZE} items OR {REDDIT_BATCH_TIMEOUT_SECONDS}s | gap {REDDIT_BATCH_GAP_SECONDS}s")
    log.info(f"  Twitter batch        : {TWITTER_BATCH_SIZE} items OR {TWITTER_BATCH_TIMEOUT_SECONDS}s | gap {TWITTER_BATCH_GAP_SECONDS}s")
    log.info(f"  Rescore batch        : {RESCORE_BATCH_SIZE} items | poll {RESCORE_POLL_INTERVAL}s | gap {RESCORE_BATCH_GAP_SECONDS}s")
    log.info(f"  Rescore source       : signals collection, status='pending' — never re-fetches, only re-scores")
    log.info(f"  Claude streaming     : True | prompt: generic 1-100 relevance/visibility/engagement")
    log.info(f"  RapidAPI config      : {bool(RAPIDAPI_KEY)} (SOLE provider — google_rank + search_volume)")
    log.info(f"  Telegram             : REMOVED")
    log.info(f"  Reddit per-post JSON/RSS-in-discovery : REMOVED (moved to flintel_google_posts + subreddit RSS)")
    log.info(f"  Reddit OAuth/PRAW    : REMOVED")
    log.info(f"  Fixed full-cycle sleep: REMOVED (each keyword + each google_post has its own independent fetch-once-forever state)")
    log.info(f"  MongoDB DB           : {MONGODB_DB}")
    log.info(f"  API auth             : {'True | ' + _working(True) if API_KEY else 'False | ' + _working(False)}")
    log.info("=" * 70)

    asyncio.run(main())
