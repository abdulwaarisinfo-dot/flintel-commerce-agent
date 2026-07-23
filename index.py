"""
FLINTEL v9.12 — Reddit (SERP Discovery decoupled from Reddit fetch via a
                NEW flintel_google_posts collection + Python auto-fuzzy
                keyword generation/filtering)
                + Twitter/X Signal Scorer
=================================================================================
Platforms : Reddit — RapidAPI SERP discovery (Google search, site:reddit.com,
            real per-post rank) -> NEW flintel_google_posts collection ->
            SEPARATE Reddit-fetch loop (public per-post RSS feed, smart-retry,
            no credentials required, fuzzy-keyword content filter)
          + Twitter (tweepy v2)

=================================================================================
WHAT CHANGED IN THIS BUILD (v9.12) — REDDIT FETCHING IS NOW FULLY DECOUPLED
FROM GOOGLE SERP DISCOVERY VIA A NEW COLLECTION. flintel_keywords AND ALL
GOOGLE-RANK / SERP CODE (search_google_for_keyword, fetch_google_rank,
fetch_search_volume, fetch_google_stats, _dig_value, _dig_list,
sync_keywords_to_db, get_due_keywords, get_keywords_missing_volume,
mark_keyword_fetched, set_keyword_retry_cooldown, seed_search_volume_batch)
ARE 100% UNTOUCHED — BYTE-FOR-BYTE IDENTICAL TO v9.11.1.
=================================================================================

  PROBLEM BEING FIXED — in v9.11.1, one keyword's SERP discovery
    (search_google_for_keyword) and that SAME keyword's Reddit RSS
    fetching (fetch_reddit_post_by_url, for every result) happened
    back-to-back inside process_one_keyword(), in the same pass, on the
    same thread. That meant: a keyword was only marked fetched=True
    (finished) once EVERY one of its Reddit posts had also been fetched
    — so a slow/flaky Reddit fetch for one keyword's posts could stall
    or distort that keyword's whole discovery cycle, and Google SERP
    data effectively "waited" on Reddit.

  FIX — Reddit fetching is now a COMPLETELY SEPARATE loop/thread reading
    from a NEW collection, `flintel_google_posts`, instead of being
    called inline from the SERP-discovery loop:

      1. SERP DISCOVERY (run_serp_discovery_loop / process_one_keyword)
         — UNCHANGED in terms of the actual Google-rank call itself
         (search_google_for_keyword() is untouched, still the sole,
         independent RapidAPI SERP call). The ONLY change here: instead
         of immediately fetching each result's Reddit RSS content
         in-line, every SERP result is saved into flintel_google_posts
         (post_url + google_rank + the exact search_keyword used +
         subreddit, extracted from the URL, + a set of Python
         auto-generated "fuzzy keywords" derived from that
         search_keyword) via save_google_post() — an insert-only
         $setOnInsert upsert, so a URL already tracked is never
         overwritten. The keyword is marked fetched=True (done, in
         flintel_keywords, exactly as before) as soon as this save step
         finishes — Google SERP storage NEVER waits on Reddit fetching
         to complete. This is the literal meaning of "decoupled": the
         SERP/rank side of the pipeline runs at its own pace regardless
         of how fast or slow Reddit is being fetched.

      2. REDDIT FETCH (run_reddit_fetch_loop) — a brand-new, fully
         independent background thread. It does NOT keep its own Python
         list of subreddits, keywords, or fuzzy keywords anywhere — it
         reads get_due_google_posts() straight off flintel_google_posts
         every pass (reddit_fetched == False), and every subreddit /
         search_keyword / fuzzy_keywords value it needs is already
         sitting on that same document (stored there by SERP discovery
         in step 1). For each due post:
           - fetch_reddit_post_by_url() is called — UNCHANGED (same
             smart-retry, jittered backoff, old.reddit.com fallback,
             RSS-only, credential-free fetch as v9.11).
           - If the HTTP fetch itself genuinely fails (retries
             exhausted), the post is left reddit_fetched=False and a
             cooldown (next_retry_at) is set via
             set_google_post_retry_cooldown() so it's retried later
             without hammering Reddit every single pass — same pacing
             philosophy as v9.11.2's keyword-level cooldown, just
             applied per-post now instead of per-keyword.
           - If the fetch succeeds, the fetched post's text (title +
             summary) is checked against that post's own stored
             fuzzy_keywords (+ its original search_keyword) via
             passes_fuzzy_filter(). This is the ONLY filtering that
             decides whether a fetched Reddit post is genuinely "about"
             the keyword it was discovered under — a Python
             auto-generated fuzzy keyword set (see
             generate_fuzzy_keywords() below), NOT a second manual
             keyword list.
           - If it matches: search_volume is read from the EXISTING,
             untouched flintel_keywords cache (looked up by
             search_keyword — same cache v9.11.1 already seeds via
             seed_search_volume_batch(), completely unchanged), stamped
             onto the item alongside google_rank / subreddit / post
             text / everything else in the EXACT same item schema as
             before, and the item is pushed into reddit_queue exactly
             as it always was — downstream batching, Claude scoring,
             and Mongo `signals` storage need ZERO changes.
           - If it does NOT match: the post is still marked
             reddit_fetched=True (the URL genuinely WAS fetched — we
             just don't want it queued), so it is never re-fetched
             again either. Only a genuine fetch FAILURE (network/HTTP)
             is retried — a successful fetch that simply isn't a topical
             match is a settled "no" and fetching it again would just
             waste requests against Reddit's IP-level rate limiting for
             no benefit.
           - reddit_fetched effectively means "False until this
             specific post URL has actually been fetched" — exactly as
             requested: a post starts as reddit_fetched=False the
             instant SERP discovery saves it, and only flips to True
             once its own fetch attempt has actually completed (success
             — matched or not — or is deliberately being retried after
             a real failure).

  Every other piece of this build — the fetch-once-forever keyword
  cache (flintel_keywords, completely untouched), the batched
  search-volume pre-seeding, the Reddit RSS smart-retry fetcher itself,
  the Claude batch scorer, the rescore processor, persistent
  batch/queue state, the FastAPI endpoints (plus one new endpoint,
  GET /google-posts, to inspect the new collection) — is kept 100%
  as-is or purely additive. No .json Reddit endpoint anywhere in this
  file — RSS (.rss) only, exactly as v9.11 established. No OAuth/PRAW.

=================================================================================
v9.12.1 PATCH NOTE (bug fix on top of v9.12, applied per user request) —
run_batch_processor() had a SECOND, redundant relevance filter
(passes_keyword_filter(text, keyword_filter_list)) that ran AFTER an item
was pulled off reddit_queue. Reddit items only ever reach reddit_queue
after ALREADY passing passes_fuzzy_filter() inside run_reddit_fetch_loop()
— that fuzzy check (against the post's own stored fuzzy_keywords + its
original search_keyword) is the single authoritative relevance decision
for Reddit. The second filter checked the fetched text against the FULL
REDDIT_SEARCH_KEYWORDS phrase list (exact full-phrase substring only) —
so any item that had matched via a fuzzy variant (a single significant
word, a bigram, or a singular/plural variant) rather than the complete
original phrase was silently dropped here: total_dropped incremented,
q.task_done() called, item discarded, current_batch.append()/
save_pending_batch() never reached. That is why items could be seen
being logged as "[REDDIT-FETCH] QUEUED" yet never appear in
flintel_pending_batch and never reach Claude scoring.

FIX — this second filter is now skipped entirely for Reddit items (the
"reddit" platform_key), since fuzzy-filtering already happened upstream
and re-checking against the full phrase list only produces false
negatives. Twitter items are NOT pre-filtered anywhere upstream, so they
still go through passes_keyword_filter() exactly as before — zero change
to Twitter's behavior. This is the ONLY functional change in this file
relative to v9.12; everything else is preserved 100% as-is.
=================================================================================
v9.12.2 PATCH NOTE (bug fix on top of v9.12.1, applied per user request) —
TWO issues in run_batch_processor(), both invisible in logs before this fix:

  BUG A — ITEM-LOSS WINDOW BETWEEN DEQUEUE AND PERSIST.
    Previously, remove_queue_message(platform_key, item.get("message_id"))
    was called IMMEDIATELY after q.get() succeeded — i.e. the instant an
    item was pulled off the in-memory reddit_queue/twitter_queue, its
    Mongo-persisted backup row in flintel_queue_messages was deleted right
    away, BEFORE it was known whether that item would be added to
    current_batch/flintel_pending_batch or dropped. If the process crashed
    or was killed in the gap between q.get() and save_pending_batch()
    (e.g. during a Mongo hiccup, an unhandled exception, a container
    restart), that item existed in NEITHER flintel_queue_messages NOR
    flintel_pending_batch — it was silently and permanently lost, and
    would not be recovered on restart (load_queue_messages() would never
    see it again, since it had already been deleted).

    FIX — remove_queue_message() is now called ONLY after the item's fate
    is fully decided AND persisted: either (a) it has been appended to
    current_batch and save_pending_batch() has successfully written that
    batch to flintel_pending_batch, or (b) it has been genuinely dropped
    for a documented, logged reason (too-short text, or — for Twitter only
    — failing passes_keyword_filter()). This closes the gap: at every
    point in time, an in-flight item exists in at least one of
    flintel_queue_messages or flintel_pending_batch, never in neither.

  BUG B — SILENT, UNTRACEABLE SHORT-TEXT DROP.
    The `if not text or len(text) < 10: q.task_done(); continue` branch
    dropped items with no log line and no counter increment
    (total_dropped was never touched here) — making it impossible to
    distinguish "item never arrived" from "item silently dropped for
    being too short" purely from the logs.

    FIX — this branch now logs a WARNING with message_id/post_url/text
    length, and increments total_dropped, exactly like the redundant-
    keyword-filter drop path already did for Twitter.

Everything else in this file — SERP discovery, Reddit fetch loop, fuzzy
keyword generation/matching, Claude batch scorer, rescore processor,
FastAPI endpoints, Mongo schemas/indexes — is preserved 100% as-is,
byte-for-byte identical to v9.12.1. Only run_batch_processor() changed.
=================================================================================

=================================================================================
v9.13 PATCH NOTE (applied per user request — ONLY these two things changed,
everything else in this file is 100% untouched from v9.12.2) —

  CHANGE 1 — LLM SCORING PROVIDER SWAPPED (Claude API -> RapidAPI GPT-5).
    _call_claude_batch() no longer calls anthropic_client.messages.stream().
    It now calls the RapidAPI "chatgpt-gpt5.p.rapidapi.com/ask" endpoint via
    plain requests.post(), sending the combined system+batch prompt as the
    "query" field, using a NEW dedicated key (CHATGPT_RAPIDAPI_KEY). The
    function's contract (input: batch list -> output: parsed list of score
    dicts) is unchanged, so score_batch_with_claude(), run_batch_processor(),
    and run_rescore_processor() needed ZERO changes. Response-text extraction
    is defensive (tries several common response shapes) since the exact
    response schema of this third-party endpoint isn't guaranteed.

  CHANGE 2 — SEPARATE RAPIDAPI KEY FOR GOOGLE SERP / RANK CALLS.
    search_google_for_keyword() and fetch_google_rank() (both call the
    google-search116.p.rapidapi.com host) now authenticate with a NEW,
    dedicated key: GOOGLE_RAPIDAPI_KEY. fetch_search_volume() /
    seed_search_volume_batch() (seo-keyword-research.p.rapidapi.com host)
    are UNTOUCHED and continue to use the original RAPIDAPI_KEY. No other
    logic, retry behavior, schema, filtering, batching, or endpoint
    structure changed anywhere in this file.
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

# ── RapidAPI — SOLE provider for search volume (seo-keyword-research host).
# UNTOUCHED from v9.11.1.
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")  # .env boht used same key
RAPIDAPI_KEYWORD_HOST = "seo-keyword-research.p.rapidapi.com"
RAPIDAPI_SEARCH_HOST  = "google-search116.p.rapidapi.com"

# ── NEW (v9.13) — dedicated RapidAPI key for the Google SERP / rank calls
# (google-search116.p.rapidapi.com), kept SEPARATE from RAPIDAPI_KEY above,
# which remains the sole key used for search-volume lookups
# (seo-keyword-research.p.rapidapi.com). Falls back to RAPIDAPI_KEY if not
# explicitly set, so existing single-key setups keep working.
GOOGLE_RAPIDAPI_KEY = os.getenv("GOOGLE_RAPIDAPI_KEY", "") or RAPIDAPI_KEY

# ── NEW (v9.13) — dedicated RapidAPI key for the GPT-5 scoring endpoint
# (chatgpt-gpt5.p.rapidapi.com), used in place of the Anthropic Claude API.
CHATGPT_RAPIDAPI_KEY = os.getenv("CHATGPT_RAPIDAPI_KEY", "")
CHATGPT_RAPIDAPI_HOST = "chatgpt-gpt5.p.rapidapi.com"
CHATGPT_RAPIDAPI_URL  = "https://chatgpt-gpt5.p.rapidapi.com/ask"
CHATGPT_TIMEOUT_SECONDS = int(os.getenv("CHATGPT_TIMEOUT_SECONDS", "120"))

# ── RapidAPI call timeouts — UNTOUCHED from v9.11.1.
DATAFORSEO_SERP_TIMEOUT_SECONDS   = int(os.getenv("DATAFORSEO_SERP_TIMEOUT_SECONDS", "120"))
DATAFORSEO_VOLUME_TIMEOUT_SECONDS = int(os.getenv("DATAFORSEO_VOLUME_TIMEOUT_SECONDS", "60"))
REDDIT_JSON_TIMEOUT_SECONDS       = int(os.getenv("REDDIT_JSON_TIMEOUT_SECONDS", "15"))  # used for the RSS fetch

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

# ── SEARCH-VOLUME RANDOM FALLBACK CONFIG — UNTOUCHED from v9.11.1. ─────────
SEARCH_VOLUME_RANDOM_FALLBACK_MIN = int(os.getenv("SEARCH_VOLUME_RANDOM_FALLBACK_MIN", "300"))
SEARCH_VOLUME_RANDOM_FALLBACK_MAX = int(os.getenv("SEARCH_VOLUME_RANDOM_FALLBACK_MAX", "5000"))


def _random_search_volume_fallback() -> int:
    """UNTOUCHED from v9.11.1."""
    return random.randint(SEARCH_VOLUME_RANDOM_FALLBACK_MIN, SEARCH_VOLUME_RANDOM_FALLBACK_MAX)


# ── REDDIT ENGAGEMENT (upvotes/comments) RANDOM FALLBACK CONFIG — UNTOUCHED.
REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN = int(os.getenv("REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN", "100"))
REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX = int(os.getenv("REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX", "3000"))


def _random_engagement_fallback() -> int:
    """UNTOUCHED from v9.11.1."""
    return random.randint(REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN, REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX)


# ── SERP DISCOVERY CONFIG (still the only source of NEW keywords) ───────────
# UNTOUCHED — this Python list's ONLY job is still to seed brand-new
# keyword documents into flintel_keywords (via sync_keywords_to_db(),
# $setOnInsert, insert-only). Nothing about how this list is consumed
# has changed.
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

      "head of talent", "head of HR", "head of people",
    "VP of people", "VP of talent", "chief people officer",
    "talent acquisition manager", "recruiting manager",
    "HR manager", "HR business partner", "people operations manager",
    "HRIS manager", "compensation and benefits manager",
    "director of talent acquisition", "director of people operations",
    "technical recruiter", "corporate recruiter", "recruiting coordinator",

      "send money to", "sending money to", "transfer money to",
    "transferring money to", "wire money to", "wiring money to",
    "move money to", "moving money to", "remit money to",
    "remitting money to", "pay my supplier", "paying my supplier",
    "pay a supplier", "paying a supplier", "pay my vendor",
    "paying my vendor", "pay my manufacturer", "pay my factory",
    "pay my partner", "pay my contractor", "pay an invoice",
    "paying an invoice", "settle an invoice", "settling an invoice",
    "pay a business", "business payment to", "supplier payment to",
    "vendor payment to", "invoice payment to", "international payment to",
    "overseas payment to", "cross border payment", "cross-border payment",
    "cross border transfer", "cross-border transfer",
    "international transfer", "international wire",
    "international wire transfer", "foreign wire transfer",
    "overseas wire transfer", "overseas transfer", "global payment",
    "global transfer", "b2b payment", "b2b transfer",
    "business to business payment",

  "send money to", "sending money to", "transfer money to",
    "transferring money to", "wire money to", "wiring money to",
    "move money to", "moving money to", "remit money to",
    "remitting money to", "pay my supplier", "paying my supplier",
    "pay a supplier", "paying a supplier", "pay my vendor",
    "paying my vendor", "pay my manufacturer", "pay my factory",
    "pay my partner", "pay my contractor", "pay an invoice",
    "paying an invoice", "settle an invoice", "settling an invoice",
    "pay a business", "business payment to", "supplier payment to",
    "vendor payment to", "invoice payment to", "international payment to",
    "overseas payment to", "cross border payment", "cross-border payment",
    "cross border transfer", "cross-border transfer",
    "international transfer", "international wire",
    "international wire transfer", "foreign wire transfer",
    "overseas wire transfer", "overseas transfer", "global payment",
    "global transfer", "b2b payment", "b2b transfer",
    "business to business payment",

  "bank blocked my", "bank blocked my transfer", "bank blocked my payment",
    "bank blocked my wire", "bank blocked my transaction",
    "bank flagged my", "bank flagged my transfer", "bank flagged my payment",
    "bank rejected my", "bank rejected my transfer", "bank rejected my payment",
    "bank declined my", "bank declined my transfer",
    "bank won't let me transfer", "bank won't let me send",
    "bank refuses to", "bank holding my", "bank holding my funds",
    "bank holding my money", "bank froze my", "account frozen",
    "funds frozen", "money frozen", "transfer frozen", "payment frozen",
    "transfer blocked", "payment blocked", "wire blocked",
    "transaction blocked", "transfer rejected", "payment rejected",
    "wire rejected", "transfer declined", "payment declined",
    "transfer failed", "payment failed", "wire failed",
    "transfer stuck", "payment stuck", "money stuck", "funds stuck",
    "money held", "funds held", "money hostage", "holding my money",
    "holding my funds", "won't release my funds", "won't release my money",
    "compliance hold", "compliance review", "compliance check",
    "AML hold", "AML review", "AML flag", "flagged for review",
    "flagged as suspicious", "suspicious activity", "suspicious transaction",
    "frozen for review", "under review", "transfer delayed",
    "payment delayed", "wire delayed", "transfer pending",
    "stuck in pending", "days to process", "weeks to process",
    "10-14 days", "10 to 14 days", "two weeks to transfer",
    "transfer taking forever", "payment taking forever",
    "money hasn't arrived", "money still hasn't arrived",
    "payment hasn't arrived", "where is my transfer",
    "where is my payment", "where is my money", "where did my money go",
    "money disappeared", "payment disappeared", "transfer disappeared",
    "no tracking", "can't track my transfer", "can't track my payment",
    "no update on my transfer", "no update on my payment",

      # ── FEE FRUSTRATION ──────────────────────────────────────────────────────
# ── BUSINESS CONTEXT ─────────────────────────────────────────────────────
    "my supplier", "my suppliers", "our supplier", "our suppliers",
    "my vendor", "my vendors", "our vendor", "our vendors",
    "my manufacturer", "my manufacturers", "our manufacturer",
    "my factory", "our factory", "my business partner",
    "our business partner", "my contractor", "our contractor",
    "my client overseas", "our client overseas",
    "import business", "importing business", "export business",
    "exporting business", "import export", "import/export",
    "importing goods", "exporting goods", "importing products",
    "exporting products", "buying from overseas", "buying from abroad",
    "sourcing from", "sourcing overseas", "sourcing abroad",
    "purchase order", "business invoice", "supplier invoice",
    "vendor invoice", "trade finance", "trade payment",
    "trade financing", "supply chain payment", "supply chain finance",
    "diaspora business", "diaspora entrepreneur",
    "running a business", "my business needs", "for my business",
    "business account", "business transfer", "business wire",
    "corporate payment", "corporate transfer", "corporate wire",
    "company payment", "company transfer", "B2B payment", "B2B transfer",
    "B2B transaction", "business to business",

  "Flintel",
  "fintech sales intelligence",
  "real time buyer signals",
  "twitter lead generation fintech",
  "cross border payment customer discovery",
  "crypto buyer intent monitoring",
  "competitor customer complaints",
  "diaspora payment communities",
  "fintech outreach automation",
  "payment failure signals",
  "how to find customers who just had a bad experience with our competitors",
  "customers are posting about payment failures but we never see it",
  "my team spends hours digging through Twitter for leads that might not even exist",
  "we lose deals because competitors reach frustrated buyers before we do",
  "most lead sources give us trash — how do I find someone actually ready now",
  "remittance and crypto customers complain publicly but we have no way to monitor or reach them",
  "we need qualified leads in real time, not a list of 500 people who might be interested",
  "our outreach messages suck — we don't know what the buyer is actually frustaurted about",
  "Clearbit or Apollo.io",
  "Static, historical B2B databases. No real-time intent signals. Buyers must proactively search or the tools miss them. High volume of irelevent contact records",
  "Drift or Conversational AI tools",
  "Website-based intent only. Misses buyers talking on Twitter, Reddit, Telegram who never visit your site. Reactive, not proactive.",
  "LinkedIn Sales Navigator",
  "Search-based, not signal-based. Misses the moment of maximum frustration or intent. Heavy manual work to identify who is actually buying now vs. passively open to conversation.",
  "Native Twitter/Reddit monitoring DIY approach",
  "Teams manually search keywords, drown in noise, miss real signals in volume. No scoring, no automation, no outreach scripts. Labor-intensive and unreliable.",
  "Traditional intent data (6sense, Demandbase)",
  "Enterprise-focused. High cost. Slow to update. Misses fintech/crypto/diaspora-specific communities (Telegram, niche Reddit subreddits) where these buyers actually congregate.",

  # ── CORRIDOR KEYWORDS ────────────────────────────────────────────────────
    "to Nigeria", "to Lagos", "to Abuja", "from Nigeria",
    "Nigeria payment", "Nigeria transfer", "Nigeria wire",
    "Nigerian supplier", "Nigerian vendor", "Nigerian manufacturer",
    "Nigeria business", "CAD to NGN", "GBP to NGN", "USD to NGN",
    "EUR to NGN", "AUD to NGN", "naira payment", "naira transfer",
    "send naira", "receive naira",
    "to Pakistan", "to Karachi", "to Lahore", "to Islamabad",
    "from Pakistan", "Pakistan payment", "Pakistan transfer",
    "Pakistan wire", "Pakistani supplier", "Pakistani vendor",
    "Pakistani manufacturer", "CAD to PKR", "GBP to PKR", "USD to PKR",
    "rupee payment", "rupee transfer",
    "to India", "to Mumbai", "to Delhi", "to Bangalore", "from India",
    "India payment", "India transfer", "India wire",
    "Indian supplier", "Indian vendor", "Indian manufacturer",
    "CAD to INR", "GBP to INR", "USD to INR",
    "to Ghana", "to Accra", "from Ghana", "Ghana payment",
    "Ghana transfer", "Ghanaian supplier", "GHS payment", "cedi payment",
    "to Kenya", "to Nairobi", "from Kenya", "Kenya payment",
    "Kenya transfer", "Kenyan supplier", "KES payment",
    "M-Pesa business", "Mpesa business",
    "to Ethiopia", "to Senegal", "to Ivory Coast", "to Cameroon",
    "to Tanzania", "to Uganda", "to Zimbabwe", "to South Africa",
    "to Johannesburg", "African supplier", "African vendor",
    "African manufacturer", "Africa payment", "Africa transfer",
    "from Canada", "from Toronto", "from Vancouver", "from Calgary",
    "from Ottawa", "from Montreal", "from UK", "from London",
    "from Manchester", "from Birmingham", "from Glasgow",
    "from USA", "from New York", "from Houston", "from Atlanta",
    "from Washington", "from Australia", "from Sydney", "from Melbourne",
    "from Perth", "from UAE", "from Dubai", "from Abu Dhabi",

    # ── AMOUNT SIGNALS ───────────────────────────────────────────────────────
    "$10,000", "$10k", "10 thousand", "$15,000", "$15k", "15 thousand",
    "$20,000", "$20k", "20 thousand", "$25,000", "$25k", "25 thousand",
    "$30,000", "$30k", "30 thousand", "$40,000", "$40k", "40 thousand",
    "$45,000", "$45k", "45 thousand", "$50,000", "$50k", "50 thousand",
    "$60,000", "$60k", "60 thousand", "$75,000", "$75k", "75 thousand",
    "$80,000", "$80k", "80 thousand", "$100,000", "$100k", "100 thousand",
    "$150,000", "$150k", "150 thousand", "$200,000", "$200k", "200 thousand",
    "$250,000", "$250k", "250 thousand", "$500,000", "$500k", "500 thousand",
    "$750,000", "$750k", "750 thousand",
    "$1 million", "$1m", "one million",
    "£10,000", "£10k", "£15,000", "£15k", "£20,000", "£20k",
    "£25,000", "£25k", "£30,000", "£30k", "£50,000", "£50k",
    "£100,000", "£100k", "£200,000", "£200k",
    "large transfer", "large amount", "large payment", "large wire",
    "large sum", "significant amount", "substantial amount",
    "big transfer", "big payment", "six figures", "seven figures",
    "six-figure", "seven-figure", "monthly volume", "weekly volume",

    # ── COMPLIANCE PAIN ──────────────────────────────────────────────────────
    "KYC rejected", "KYC failed", "KYC verification failed",
    "KYC problem", "KYC issue", "KYC nightmare",
    "AML rejected", "AML flagged", "AML hold", "AML review",
    "documentation rejected", "documents rejected",
    "proof of funds", "source of funds", "source of wealth",
    "proof of business", "business verification failed",
    "verification rejected", "verification failed",
    "compliance rejected", "compliance hold", "compliance review",
    "compliance nightmare", "compliance problem", "compliance issue",
    "Form M", "CBN compliance", "regulatory hold", "regulatory review",
    "regulatory problem", "regulatory issue",
    "submitted documents again", "sent documents again",
    "asking for documents again", "same documents again",
    "keep asking for documents", "keep rejecting documents",
    "third time submitting", "fourth time submitting",
    "rejected again", "blocked again", "failed again",
    "happening again", "third time", "fourth time",
    "keep blocking", "keeps blocking", "keeps rejecting", "keeps failing",
    "always blocks", "always rejects", "always fails",

    # ── URGENCY SIGNALS ──────────────────────────────────────────────────────
    "urgently", "urgent", "desperately", "desperate",
    "ASAP", "as soon as possible", "right now", "today",
    "this week", "by Friday", "by Monday", "by end of week",
    "by end of month", "deadline", "time sensitive",
    "need it done", "need it now", "need it today", "need it urgently",
    "waiting on payment", "supplier is waiting", "supplier waiting",
    "vendor is waiting", "vendor waiting", "manufacturer waiting",
    "partner waiting", "been waiting", "already delayed", "already late",
    "overdue", "past due", "losing the contract", "losing my supplier",
    "losing my vendor", "threatening to cancel", "might cancel",
    "going to cancel", "cancelling the order", "losing the deal",
    "deal at risk", "relationship at risk",
    "can't wait any longer", "running out of time", "no more time",

    # ── BUSINESS EXPANSION ───────────────────────────────────────────────────
    "just signed a supplier", "signed a new supplier", "found a supplier",
    "new supplier in", "signed a contract with", "new contract with",
    "starting to import", "starting an import", "starting to export",
    "starting an export", "launching in", "expanding to",
    "entering the market", "new market", "setting up payments",
    "need to set up payments", "need to transfer money",
    "will need to send", "will need to transfer", "going to need",
    "starting a business", "new business", "import business",
    "export business", "trading company", "sourcing products from",
    "sourcing goods from", "buying products from", "buying goods from",
    "manufacturing in", "producing in",

    # ── TREASURY & FX ────────────────────────────────────────────────────────
    "treasury management", "cash management", "liquidity management",
    "FX management", "FX exposure", "FX risk", "FX hedging",
    "currency hedging", "currency risk", "currency exposure",
    "FX solution", "FX platform", "FX tool",
    "treasury solution", "treasury platform", "cash flow management",
    "multi currency", "multi-currency", "multicurrency",
    "currency account", "foreign currency account",
    "international banking", "international bank account",
    "global banking", "global bank account", "correspondent banking",
    "banking relationship", "banking partner",
    "payment infrastructure", "payment rails", "payment solution",
    "payment platform", "payment provider", "payment partner",
    "fintech payment", "embedded payment", "embedded finance",
    "cross border banking", "international banking solution",
    "FX banking", "FX banking relationship", "FX liquidity",
    "cash pooling", "cash concentration",
    "intercompany payment", "intercompany transfer",

    # ── JOB SIGNALS ──────────────────────────────────────────────────────────
    "treasury manager", "treasury analyst", "FX manager", "FX analyst",
    "FX trader", "treasury director", "head of treasury", "VP treasury",
    "international payments manager", "global payments manager",
    "cross border payments", "payments operations manager",
    "payments specialist", "treasury specialist", "FX specialist",
    "international finance manager", "global finance manager",
    "head of payments", "director of payments", "VP payments",
    "chief financial officer", "head of finance", "finance director",
    "controller international", "global controller",

]

# ── PER-KEYWORD "FETCH ONCE, EVER" CACHE CONFIG — UNTOUCHED from v9.11.1. ──
KEYWORD_CHECK_INTERVAL_SECONDS  = int(os.getenv("KEYWORD_CHECK_INTERVAL_SECONDS", "60"))

# ── KEYWORD RETRY COOLDOWN — UNTOUCHED. Kept purely so flintel_keywords'
# schema/behavior stays byte-for-byte identical to v9.11.1, even though
# this specific cooldown path is no longer exercised by the SERP loop
# now that Reddit fetching has moved out of process_one_keyword() (SERP
# discovery no longer performs any Reddit HTTP fetch that could fail).
REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS = int(os.getenv("REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS", "1800"))

SERP_RESULTS_PER_KEYWORD = int(os.getenv("SERP_RESULTS_PER_KEYWORD", "20"))
SERP_MONTHS_BACK         = int(os.getenv("SERP_MONTHS_BACK", "6"))
SERP_FETCH_SLEEP_SECONDS = float(os.getenv("SERP_FETCH_SLEEP_SECONDS", "1.5"))

# ── SEARCH-VOLUME BATCH SEEDING CONFIG — UNTOUCHED. ─────────────────────────
SEARCH_VOLUME_BATCH_SIZE = int(os.getenv("SEARCH_VOLUME_BATCH_SIZE", "12"))

# ── TWITTER SEARCH KEYWORDS — independent from Reddit's list, unchanged ────
TWITTER_SEARCH_KEYWORDS = [
    kw.strip() for kw in os.getenv(
        "TWITTER_SEARCH_KEYWORDS",
        "Wise blocked,bank blocked my transfer,Payoneer blocked,"
        "cross border payment,CRM is a nightmare,recommend a CRM,"
        "we got hacked,ransomware attack,need incident response,"
        "Salesforce alternative,switching from HubSpot"
    ).split(",") if kw.strip()
]

# ── REDDIT "SMART FETCH" CONFIG — v9.6 retry logic, UNCHANGED. Still used
# by fetch_reddit_post_by_url() / _reddit_get_with_retry(), now called
# from run_reddit_fetch_loop() instead of from the SERP loop — the retry
# behavior itself is identical either way.
REDDIT_FETCH_MAX_RETRIES     = int(os.getenv("REDDIT_FETCH_MAX_RETRIES", "3"))
REDDIT_FETCH_BACKOFF_BASE    = float(os.getenv("REDDIT_FETCH_BACKOFF_BASE", "2.0"))
REDDIT_FETCH_JITTER_MIN      = float(os.getenv("REDDIT_FETCH_JITTER_MIN", "0.4"))
REDDIT_FETCH_JITTER_MAX      = float(os.getenv("REDDIT_FETCH_JITTER_MAX", "1.6"))
REDDIT_USER_AGENT = os.getenv(
    "REDDIT_USER_AGENT",
    "python:flintel-signal-bot:v9.12 (by /u/flintel_signals)",
)

# ── NEW (v9.12) — flintel_google_posts CONFIG ───────────────────────────────
# REDDIT_FETCH_CHECK_INTERVAL_SECONDS -> how often run_reddit_fetch_loop()
#                        wakes up to ask "are there any flintel_google_posts
#                        documents still reddit_fetched=False?" Cheap DB
#                        query — the actual Reddit RSS HTTP fetch only fires
#                        for posts genuinely due.
#
# REDDIT_POST_RETRY_COOLDOWN_SECONDS -> when a specific post_url's Reddit
#                        RSS fetch genuinely fails (network/HTTP, retries
#                        exhausted), it is left reddit_fetched=False so it
#                        gets retried, but not on the very next pass —
#                        next_retry_at spaces retries out exactly like
#                        v9.11.2's per-keyword cooldown did, just scoped to
#                        one post_url instead of one keyword now.
REDDIT_FETCH_CHECK_INTERVAL_SECONDS = int(os.getenv("REDDIT_FETCH_CHECK_INTERVAL_SECONDS", "30"))
REDDIT_POST_RETRY_COOLDOWN_SECONDS  = int(os.getenv("REDDIT_POST_RETRY_COOLDOWN_SECONDS", "1800"))

SERP_RESULTS_PER_KEYWORD = SERP_RESULTS_PER_KEYWORD  # unchanged reference kept for clarity

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
# GENERIC JSON FIELD-EXTRACTION HELPERS — UNTOUCHED from v9.11.1. Used ONLY
# by the Google-rank / search-volume RapidAPI code below, which this build
# does not modify in any way.
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
    """Generic keyword gate — UNCHANGED in implementation. Still used as
    a second-layer safety filter inside run_batch_processor() before a
    batch is sent to Claude — but as of v9.12.1 it is only actually
    invoked for Twitter items (see run_batch_processor() below). Reddit
    items are pre-filtered upstream by passes_fuzzy_filter(), which is
    the authoritative relevance check for that platform."""
    t = text.lower()
    for kw in keywords:
        if kw.lower() in t:
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# NEW (v9.12) — PYTHON AUTO-FUZZY-KEYWORD GENERATION + MATCHING
#
# These two functions are the entire "fuzzy keyword" system requested:
# generate_fuzzy_keywords() runs ONCE per SERP result, at save time, and
# the resulting list is stored directly on that post's flintel_google_posts
# document (see save_google_post() below) — nothing is regenerated later,
# nothing is kept in a separate python list. passes_fuzzy_filter() is the
# matcher used by run_reddit_fetch_loop() against the ACTUAL fetched RSS
# content, using exactly the fuzzy_keywords + search_keyword already
# stored on that one document.
# ─────────────────────────────────────────────────────────────────────────────

_FUZZY_STOPWORDS = {
    "a", "an", "the", "to", "for", "of", "in", "on", "my", "our", "is",
    "are", "and", "or", "with", "from", "at", "by", "your", "their",
}


def generate_fuzzy_keywords(search_keyword: str) -> list:
    """
    Python auto-generates a small set of fuzzy variants for one Google
    search_keyword, so Reddit-fetch-time filtering isn't limited to an
    exact-substring match against the full original phrase. This is
    intentionally simple/deterministic (no external NLP dependency):

      - the full original phrase, lowercased
      - the phrase with stopwords stripped
      - every individual "significant" word (len > 2, not a stopword)
      - every consecutive significant-word bigram, in original order
      - a naive singular/plural variant of every value above

    Called exactly once per SERP result, at save_google_post() time —
    the result is persisted on that post's own flintel_google_posts
    document and reused from there every time that post is considered
    for fetching. Never regenerated on the fly, never kept in a
    standalone python list.
    """
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
    """
    Checks fetched Reddit post text (title + summary, as produced by
    fetch_reddit_post_by_url()) against the ORIGINAL search_keyword and
    that post's own stored fuzzy_keywords list — both read straight off
    the flintel_google_posts document, nothing recomputed here. Simple
    substring containment, same style as the existing
    passes_keyword_filter() used downstream in the batch processor.
    """
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
# TWITTER SEARCH QUERY — built directly from TWITTER_SEARCH_KEYWORDS
# (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _build_twitter_search_query() -> str:
    if not TWITTER_SEARCH_KEYWORDS:
        return (
            "(\"international transfer\" OR \"bank blocked\" OR \"CRM is a nightmare\")"
            " -is:retweet lang:en"
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
# NEW (v9.12): flintel_google_posts.
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

        # ── flintel_keywords — FETCH-ONCE-FOREVER cache. UNTOUCHED
        # collection/index definitions from v9.11.1 — this build does not
        # modify this collection's schema, indexes, or logic in any way.
        db.flintel_keywords.create_index([("keyword", ASCENDING)], unique=True, name="keyword_unique")
        db.flintel_keywords.create_index([("fetched", ASCENDING)], name="keyword_fetched_idx")
        db.flintel_keywords.create_index([("search_volume", ASCENDING)], name="keyword_volume_idx")
        db.flintel_keywords.create_index([("next_retry_at", ASCENDING)], name="keyword_retry_cooldown_idx")

        # ── NEW (v9.12) — flintel_google_posts. One document per Reddit
        # post_url ever surfaced by SERP discovery. Stores everything
        # Reddit-fetch needs (post_url, google_rank, the exact
        # search_keyword used to find it, its subreddit, and its
        # Python-generated fuzzy_keywords) so run_reddit_fetch_loop()
        # never has to keep its own parallel python list of any of this
        # — it reads it straight off these documents.
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

# ─────────────────────────────────────────────────────────────────────────────
# ANTHROPIC CLIENT — kept for compatibility, no longer used for scoring as of
# v9.13 (scoring now goes through the RapidAPI GPT-5 endpoint — see
# _call_claude_batch() below). Left in place untouched since it is not one of
# the two things requested to change.
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
# PERSISTENT BATCH STATE HELPERS — UNCHANGED from v9.11.1.
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
# KEYWORD CACHE — flintel_keywords collection. 100% UNTOUCHED from v9.11.1
# — every function below is byte-for-byte identical to v9.11.1. Still the
# ONLY thing search_volume is ever sourced from (looked up by keyword from
# run_reddit_fetch_loop() now, instead of from process_one_keyword()).
# ─────────────────────────────────────────────────────────────────────────────

def sync_keywords_to_db(keywords: list):
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
# SEARCH-VOLUME BATCH SEEDING — 100% UNTOUCHED from v9.11.1. Still uses the
# original RAPIDAPI_KEY (seo-keyword-research.p.rapidapi.com host) — NOT the
# new GOOGLE_RAPIDAPI_KEY, which is only for the google-search116 SERP host.
# ─────────────────────────────────────────────────────────────────────────────

def seed_search_volume_batch(keywords_needing_volume: list, batch_size: int = SEARCH_VOLUME_BATCH_SIZE):
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
# ENRICHMENT.
#
# v9.13 CHANGE 2: fetch_search_volume() (seo-keyword-research host) is
# UNTOUCHED and still uses RAPIDAPI_KEY. fetch_google_rank() (google-search116
# host) now uses the NEW, dedicated GOOGLE_RAPIDAPI_KEY instead. Nothing else
# in either function changed.
# ─────────────────────────────────────────────────────────────────────────────

def fetch_search_volume(search_keyword: str) -> int | None:
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
    # v9.13 CHANGE 2: uses GOOGLE_RAPIDAPI_KEY (dedicated key), not RAPIDAPI_KEY.
    if not GOOGLE_RAPIDAPI_KEY or not search_keyword:
        return None
    try:
        url = "https://google-search116.p.rapidapi.com/"

        querystring = {"query": search_keyword}

        headers = {
            "x-rapidapi-key": GOOGLE_RAPIDAPI_KEY, # .env — dedicated Google SERP key (v9.13)
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
# (site:reddit.com) -> real per-post rank + URL.
#
# v9.13 CHANGE 2: this function now authenticates with the dedicated
# GOOGLE_RAPIDAPI_KEY instead of RAPIDAPI_KEY (same google-search116 host,
# same query shape, same result parsing — only the key changed). Everything
# else about what process_one_keyword() does with its results (saving into
# flintel_google_posts instead of fetching Reddit RSS in-line) is unchanged.
# ─────────────────────────────────────────────────────────────────────────────

def search_google_for_keyword(keyword: str, months_back: int = SERP_MONTHS_BACK) -> list:
    if not GOOGLE_RAPIDAPI_KEY:
        log.warning("[SERP] Google RapidAPI key not set — skipping SERP search.")
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
            "x-rapidapi-key": GOOGLE_RAPIDAPI_KEY, # .env — dedicated Google SERP key (v9.13)
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
    """UNCHANGED — checks `signals` directly by post_url before any
    Reddit fetch or Claude scoring happens, now consulted from
    run_reddit_fetch_loop() instead of process_one_keyword()."""
    if not post_url:
        return False
    try:
        existing = db.signals.find_one({"post_url": post_url}, {"_id": 1})
        return existing is not None
    except Exception as exc:
        log.error(f"[DEDUP] is_post_already_signaled error for {post_url}: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# NEW (v9.12) — flintel_google_posts HELPERS
#
# This collection is the single source of truth for "which Reddit post_url
# has SERP discovery found, and has it actually been Reddit-fetched yet?"
# It is populated ONLY by save_google_post() (called from
# process_one_keyword(), right after search_google_for_keyword() returns),
# and consumed ONLY by run_reddit_fetch_loop() below. Neither side keeps its
# own separate python list of subreddits/keywords/fuzzy-keywords — everything
# lives on these documents.
# ─────────────────────────────────────────────────────────────────────────────

def save_google_post(post_url: str, google_rank, search_keyword: str, subreddit: str, fuzzy_keywords: list) -> bool:
    """
    Insert-only upsert (mirrors the exact same $setOnInsert pattern
    flintel_keywords already uses) — a post_url already tracked here is
    NEVER overwritten, so re-discovering the same URL under a different
    keyword search later does not reset its reddit_fetched state or
    swap out its original search_keyword/fuzzy_keywords. Returns True
    only when this call genuinely inserted a brand-new document (used
    purely for the "X new posts saved" log line in process_one_keyword).
    """
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
    """
    Returns every flintel_google_posts document that is still
    reddit_fetched=False AND not currently in a retry cooldown. This is
    read DIRECTLY from Mongo every pass — run_reddit_fetch_loop() never
    caches or mirrors this into a python list of its own; each returned
    document already carries its own post_url, google_rank,
    search_keyword, subreddit, and fuzzy_keywords, which is everything
    the fetch step needs.
    """
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
    """
    Flips reddit_fetched=True PERMANENTLY for this post_url — it will
    never be re-fetched again, whether or not its content actually
    matched the fuzzy keywords (fuzzy_matched is stored either way, for
    later inspection via GET /google-posts). This is only called after
    a genuinely COMPLETED fetch attempt (the RSS request itself
    succeeded) — a real HTTP/network failure instead calls
    set_google_post_retry_cooldown() and leaves reddit_fetched=False so
    it is retried later.
    """
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
    """
    Called when a specific post_url's Reddit RSS fetch genuinely failed
    (fetch_reddit_post_by_url() returned None — retries exhausted).
    Keeps reddit_fetched=False (it WILL be retried) but stamps
    next_retry_at so get_due_google_posts() skips it until the cooldown
    passes, instead of hammering the same URL on the very next
    REDDIT_FETCH_CHECK_INTERVAL_SECONDS pass — same pacing principle as
    v9.11.2's per-keyword cooldown, scoped to one post_url here.
    """
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
# UNCHANGED from v9.11 in terms of retry/backoff/parsing behavior — only
# the CALLER changed (run_reddit_fetch_loop() instead of
# process_one_keyword()). No .json endpoint anywhere in this file.
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
    """
    UNCHANGED from v9.11 — public, credential-free per-post RSS feed
    (post_url + ".rss"), same smart-retry + old.reddit.com fallback host.
    Engagement (upvotes/comments) is still a clearly-logged random
    fallback (RSS exposes no real counts). Now called from
    run_reddit_fetch_loop() instead of process_one_keyword() — the
    function body itself is untouched.
    """
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

        upvotes = _random_engagement_fallback()
        comments = _random_engagement_fallback()
        log.warning(
            f"[REDDIT-FETCH] RANDOM FALLBACK applied for engagement on {post_url} | "
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
            "engagement_is_random": True,
            "google_rank":          rank,
            "search_volume":        None,   # filled in by run_reddit_fetch_loop() below
        }
    except Exception as exc:
        log.error(f"[REDDIT-FETCH] fetch_reddit_post_by_url parse error for {post_url}: {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# NEW (v9.12) — process_one_keyword() no longer fetches Reddit at all.
# It ONLY runs search_google_for_keyword() and immediately persists every
# result into flintel_google_posts. Google SERP data is saved and that
# keyword is marked done WITHOUT waiting on any Reddit HTTP call whatsoever.
# ─────────────────────────────────────────────────────────────────────────────

def process_one_keyword(keyword: str) -> tuple:
    """
    Full SERP-discovery work for ONE keyword that get_due_keywords() has
    flagged as due right now:
      1. RapidAPI SERP search (site:reddit.com, last N months) — same
         search_google_for_keyword() call as before (now authenticated
         with GOOGLE_RAPIDAPI_KEY as of v9.13 — see that function).
      2. For every result: generate that result's fuzzy keywords
         (generate_fuzzy_keywords(), run once here) and save it into
         flintel_google_posts via save_google_post() — insert-only, so
         a post_url already tracked (e.g. from a previous keyword whose
         SERP results happened to overlap) is left completely alone.

    Reddit is NEVER fetched here. This keyword's SERP job is considered
    complete the moment this function returns — Google SERP storage
    never waits on Reddit RSS fetching, which now happens entirely on
    its own schedule in run_reddit_fetch_loop().

    Returns (results_count, new_posts_saved_count) for logging.
    """
    results = search_google_for_keyword(keyword, months_back=SERP_MONTHS_BACK)

    new_posts_saved = 0
    for result in results:
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
    """
    Continuously polls flintel_keywords every KEYWORD_CHECK_INTERVAL_SECONDS
    for keywords that have NEVER been fetched (fetched=False), and for any
    keyword still missing a cached search_volume (batch-seeds it) —
    UNCHANGED behavior from v9.11.1 in every respect except one: each due
    keyword's SERP results are now saved into flintel_google_posts by
    process_one_keyword() instead of being fetched from Reddit in-line, so
    there is no more had_fetch_failure concept at the keyword level —
    mark_keyword_fetched() is now called unconditionally once
    process_one_keyword() returns, since nothing about Reddit's
    availability can cause this SERP step itself to "fail" anymore.
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
        f"KEYWORD CACHE: fetch-once-forever, restart-safe, no re-fetch ever (UNTOUCHED) | "
        f"SEARCH-VOLUME: batched loop (size {SEARCH_VOLUME_BATCH_SIZE}), random fallback range "
        f"{SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-{SEARCH_VOLUME_RANDOM_FALLBACK_MAX} on failure "
        f"(UNTOUCHED) | REDDIT FETCH: fully decoupled — SERP results are only SAVED into "
        f"flintel_google_posts here, the actual Reddit RSS fetch happens in a separate loop"
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

            total_results, total_new_posts = 0, 0
            for doc in due:
                keyword = doc["keyword"]
                results_count, new_posts_saved = process_one_keyword(keyword)
                total_results += results_count
                total_new_posts += new_posts_saved

                # No Reddit fetch happens in this loop anymore, so there is
                # no failure mode here to leave this keyword pending for —
                # mark it done unconditionally, exactly as soon as its SERP
                # results are saved into flintel_google_posts.
                mark_keyword_fetched(keyword)
                log.info(
                    f"[SERP] '{keyword}' DONE | serp_results:{results_count} | "
                    f"new_google_posts_saved:{new_posts_saved} | "
                    f"marked fetched=True PERMANENTLY (Reddit fetch happens separately, "
                    f"asynchronously, from flintel_google_posts — not waited on here)"
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
# NEW (v9.12) — run_reddit_fetch_loop(): the entire Reddit-fetch side of
# the pipeline, fully independent of run_serp_discovery_loop(). Reads
# EVERYTHING it needs (post_url, google_rank, search_keyword, subreddit,
# fuzzy_keywords) straight off flintel_google_posts documents — no
# separate python list of subreddits/keywords/fuzzy-keywords is kept
# anywhere in this loop.
# ─────────────────────────────────────────────────────────────────────────────

def run_reddit_fetch_loop():
    log.info(
        f"[REDDIT-FETCH] Loop started | reads directly from flintel_google_posts, "
        f"NOT from any python list | check_interval:{REDDIT_FETCH_CHECK_INTERVAL_SECONDS}s | "
        f"retry_cooldown:{REDDIT_POST_RETRY_COOLDOWN_SECONDS}s | "
        f"fetch method: public per-post RSS only, credential-free "
        f"({REDDIT_FETCH_MAX_RETRIES}x backoff + old.reddit.com fallback, no OAuth/PRAW) | "
        f"search_volume for every queued item is read from the UNTOUCHED flintel_keywords cache"
    )

    while True:
        try:
            due_posts = get_due_google_posts()
            if not due_posts:
                time.sleep(REDDIT_FETCH_CHECK_INTERVAL_SECONDS)
                continue

            log.info(f"[REDDIT-FETCH] {len(due_posts)} post(s) due for Reddit RSS fetch this pass")

            queued_count, no_match_count, dupe_count, fail_count = 0, 0, 0, 0

            for doc in due_posts:
                post_url       = doc["post_url"]
                search_keyword = doc.get("search_keyword", "")
                fuzzy_keywords = doc.get("fuzzy_keywords", [])
                subreddit      = doc.get("subreddit", "")
                google_rank    = doc.get("google_rank")

                if is_post_already_signaled(post_url):
                    mark_google_post_fetched(post_url, fuzzy_matched=None)
                    dupe_count += 1
                    log.info(f"[REDDIT-FETCH] SKIP (already in signals) | {post_url}")
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
                        f"marked reddit_fetched=True (not queued — settled 'no', won't be retried)"
                    )
                    time.sleep(SERP_FETCH_SLEEP_SECONDS)
                    continue

                # ── MATCH — pull search_volume from the UNTOUCHED
                # flintel_keywords cache (already seeded by
                # seed_search_volume_batch(), completely unmodified),
                # stamp everything onto the item in the exact same
                # schema as before, and queue it exactly as always.
                kw_doc = db.flintel_keywords.find_one({"keyword": search_keyword})
                volume = kw_doc.get("search_volume") if kw_doc else None
                volume_is_random = kw_doc.get("search_volume_is_random", False) if kw_doc else False

                item["search_volume"] = volume
                item["search_volume_is_random"] = volume_is_random
                item["subreddit_or_channel"] = subreddit or item.get("subreddit_or_channel", "")

                reddit_queue.put(item)
                save_queue_message("reddit", item)
                mark_google_post_fetched(post_url, fuzzy_matched=True)
                queued_count += 1

                sv_tag = "RANDOM-FALLBACK" if volume_is_random else "real"
                log.info(
                    f"[REDDIT-FETCH] QUEUED | {post_url} | keyword:{search_keyword!r} | "
                    f"subreddit:{subreddit!r} | google_rank:{google_rank} | "
                    f"search_volume:{volume} ({sv_tag}, from flintel_keywords cache) | "
                    f"marked reddit_fetched=True PERMANENTLY"
                )
                time.sleep(SERP_FETCH_SLEEP_SECONDS)

            log.info(
                f"[REDDIT-FETCH] Pass complete | due:{len(due_posts)} | queued:{queued_count} | "
                f"no_fuzzy_match:{no_match_count} | already_signaled:{dupe_count} | "
                f"failed_will_retry:{fail_count}"
            )

        except Exception as exc:
            log.error(f"[REDDIT-FETCH] loop error: {exc}")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────
# LLM BATCH SCORER — streaming transport + partial-JSON recovery.
#
# v9.13 CHANGE 1: _call_claude_batch() no longer calls the Anthropic Claude
# API. It now calls the RapidAPI GPT-5 "/ask" endpoint
# (chatgpt-gpt5.p.rapidapi.com) via a plain requests.post(), using the NEW
# dedicated CHATGPT_RAPIDAPI_KEY. The function's public contract is
# unchanged (batch list in -> parsed list of score dicts out), so
# score_batch_with_claude(), run_batch_processor(), and
# run_rescore_processor() below needed ZERO changes — they still just call
# score_batch_with_claude(). Prompt-building, code-fence stripping, partial-
# JSON salvage, truncation handling, and score clamping are all UNCHANGED
# from v9.12.2.
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
                    log.warning("[LLM-Batch] Skipped one malformed salvaged object.")
                obj_start = None
        i += 1
    return objects


def _parse_claude_json(raw: str) -> tuple:
    cleaned = _strip_code_fences(raw)
    try:
        parsed = json.loads(cleaned)
        if not isinstance(parsed, list):
            raise ValueError("LLM returned non-list.")
        return parsed, False
    except (json.JSONDecodeError, ValueError) as exc:
        log.warning(f"[LLM-Batch] Full parse failed ({exc}) — attempting partial recovery.")
        return _salvage_partial_json_array(cleaned), True


def _extract_gpt_rapidapi_text(data) -> str:
    """
    NEW (v9.13). The chatgpt-gpt5.p.rapidapi.com/ask endpoint's exact
    response shape isn't guaranteed the same way the Anthropic SDK's
    was, so this defensively tries the common shapes third-party GPT
    RapidAPI wrappers use, in order, before giving up:
      - {"result": "..."}                              (as documented by
                                                          the user-provided
                                                          snippet)
      - {"response": "..."}
      - {"answer": "..."}
      - {"text": "..."}
      - {"output": "..."}
      - {"message": "..."}
      - {"choices": [{"message": {"content": "..."}}]}  (OpenAI-style)
      - {"choices": [{"text": "..."}]}
      - a bare JSON string response
    Falls back to str(data) if nothing recognizable is found, so
    downstream JSON parsing/salvage still gets *something* to work with
    (and will simply fail-safe into _fallback_score() entries if that
    text truly isn't usable).
    """
    if isinstance(data, str):
        return data

    if isinstance(data, dict):
        for key in ("result", "response", "answer", "text", "output", "message", "content"):
            val = data.get(key)
            if isinstance(val, str) and val.strip():
                return val

        choices = data.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0]
            if isinstance(first, dict):
                msg = first.get("message")
                if isinstance(msg, dict) and isinstance(msg.get("content"), str):
                    return msg["content"]
                if isinstance(first.get("text"), str):
                    return first["text"]

        data_field = data.get("data")
        if isinstance(data_field, str) and data_field.strip():
            return data_field
        if isinstance(data_field, dict):
            nested = _extract_gpt_rapidapi_text(data_field)
            if nested:
                return nested

    return str(data)


def _call_claude_batch(batch: list) -> list:
    """
    NAME KEPT AS _call_claude_batch() for zero-change compatibility with
    score_batch_with_claude() below — as of v9.13 this calls the RapidAPI
    GPT-5 "/ask" endpoint instead of the Anthropic Claude API.
    """
    prompt = _build_batch_prompt(batch)
    full_query = f"{CLAUDE_SYSTEM_PROMPT}\n\nScore this batch:\n\n{prompt}"

    payload = {"query": full_query}
    headers = {
        "x-rapidapi-key": CHATGPT_RAPIDAPI_KEY,
        "x-rapidapi-host": CHATGPT_RAPIDAPI_HOST,
        "Content-Type": "application/json"
    }

    resp = requests.post(
        CHATGPT_RAPIDAPI_URL,
        json=payload,
        headers=headers,
        timeout=CHATGPT_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()

    try:
        data = resp.json()
    except ValueError:
        raw = resp.text
    else:
        raw = _extract_gpt_rapidapi_text(data)

    raw = (raw or "").strip()

    results, was_truncated = _parse_claude_json(raw)

    if was_truncated:
        recovered = {int(r["index"]) for r in results if isinstance(r, dict) and "index" in r}
        missing = sorted(set(range(1, len(batch) + 1)) - recovered)
        log.warning(f"[LLM-Batch] PARTIAL RECOVERY | batch_size:{len(batch)} | "
                    f"recovered:{len(recovered)} | missing:{len(missing)}")
        log_operator_alert(
            title="LLM Response Truncated/Unparseable — Partial Recovery",
            detail=f"batch_size:{len(batch)} recovered:{len(recovered)} missing:{missing[:30]}",
            level="ERROR",
        )
        for idx in missing:
            results.append(_fallback_score(idx, "Truncated — not recovered."))

    if not isinstance(results, list):
        raise ValueError("LLM returned non-list after parsing.")

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
    result = retry_with_backoff(_call_claude_batch, batch, retries=3, delay=5, label="LLM-Batch")
    if result is None:
        log_operator_alert(
            title="LLM API Unavailable",
            detail=f"All 3 retry attempts failed for a batch of {len(batch)} items.",
            level="CRITICAL",
        )
        return [_fallback_score(i + 1, "LLM API unavailable after 3 retries.") for i in range(len(batch))]
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB STORAGE — UNCHANGED from v9.11.1.
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
#
# v9.12.2 PATCH (preserved as-is): remove_queue_message() is called ONLY
# after an item's fate is fully decided AND persisted (either appended to
# current_batch + save_pending_batch() succeeded, or genuinely dropped for
# a logged reason). The too-short-text drop path is logged and counted in
# total_dropped. Batching logic, timeout/gap handling, enrichment, and the
# LLM call are otherwise 100% UNCHANGED from v9.12.1 — including the
# v9.12.1 fix that skips passes_keyword_filter() for Reddit items. Nothing
# in this function changed for v9.13 (score_batch_with_claude()'s contract
# is unchanged).
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
                # NOTE (v9.12.2): remove_queue_message() is intentionally
                # NOT called here anymore. It is now called further below,
                # only once this item's fate (added to a persisted batch,
                # or genuinely dropped) has been decided AND written to
                # Mongo — so the item always exists in at least one of
                # flintel_queue_messages / flintel_pending_batch until it
                # is fully accounted for. This closes the item-loss window
                # that previously existed between q.get() and
                # save_pending_batch()/drop.
                message_id = item.get("message_id")

                text = (item.get("text") or "").strip()

                if not text or len(text) < 10:
                    total_dropped += 1
                    log.warning(
                        f"[{platform_label}] DROPPED (text too short: {len(text)} char(s), "
                        f"min 10 required) | message_id:{message_id} | "
                        f"post_url:{item.get('post_url', '')!r}"
                    )
                    remove_queue_message(platform_key, message_id)
                    q.task_done()
                    continue

                # v9.12.1 FIX (preserved as-is) — Reddit items only ever
                # reach this queue after already passing
                # passes_fuzzy_filter() in run_reddit_fetch_loop() (matched
                # against that post's own stored fuzzy_keywords + original
                # search_keyword — the authoritative relevance decision for
                # Reddit). Re-checking here against the FULL
                # REDDIT_SEARCH_KEYWORDS phrase list (exact full-phrase
                # substring only) was silently dropping items that had
                # matched via a fuzzy variant rather than the complete
                # original phrase — they never reached
                # current_batch/save_pending_batch(), so they never showed
                # up in flintel_pending_batch and never got scored. Twitter
                # items are never pre-filtered upstream, so this filter
                # still applies to them exactly as before.
                if platform_key != "reddit" and not passes_keyword_filter(text, keyword_filter_list):
                    total_dropped += 1
                    log.info(
                        f"[{platform_label}] DROPPED (failed keyword filter) | "
                        f"message_id:{message_id}"
                    )
                    remove_queue_message(platform_key, message_id)
                    q.task_done()
                    continue

                total_matched += 1
                if not current_batch:
                    batch_start_time = time.time()

                current_batch.append(item)
                save_pending_batch(platform_key, current_batch, batch_start_time)
                save_batch_seconds(platform_key, batch_start_time)

                # Only remove the item from its persistent queue-store
                # backup AFTER save_pending_batch() has successfully
                # written it into flintel_pending_batch — at no point in
                # time is the item absent from both collections.
                remove_queue_message(platform_key, message_id)

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
# RESCORE PROCESSOR — UNCHANGED from v9.11.1 (still calls
# score_batch_with_claude(), whose contract is unchanged in v9.13).
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
# TWITTER / X POLLER — UNCHANGED from v9.11.1.
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
    Reddit now runs on THREE independent threads instead of two:
      1. SERP discovery (run_serp_discovery_loop) — Google call (now via
         GOOGLE_RAPIDAPI_KEY as of v9.13), saves results into
         flintel_google_posts, never waits on Reddit.
      2. Reddit fetch (run_reddit_fetch_loop) — reads flintel_google_posts
         directly, fetches RSS, fuzzy-filters, queues.
      3. Batch processor (run_batch_processor) — consumes reddit_queue
         exactly as before, with the v9.12.1/v9.12.2 fixes, scoring now
         via the RapidAPI GPT-5 endpoint (v9.13).
    Governed entirely by REDDIT_ENABLED + GOOGLE_RAPIDAPI_KEY (required for
    SERP discovery; the per-post RSS fetch step itself needs no
    credentials at all).
    """
    if not REDDIT_ENABLED:
        log.warning("Reddit platform DISABLED — skipping.")
        return
    if not GOOGLE_RAPIDAPI_KEY:
        log.warning("Reddit not started — GOOGLE_RAPIDAPI_KEY not set (required for SERP discovery).")
        return

    resumed = load_queue_messages("reddit")
    for it in resumed:
        reddit_queue.put(it)
    if resumed:
        log.info(f"[REDDIT] Resumed {len(resumed)} queue message(s) from MongoDB after restart.")

    serp_thread = threading.Thread(target=run_serp_discovery_loop, daemon=True, name="Reddit-SERP")
    fetch_thread = threading.Thread(target=run_reddit_fetch_loop, daemon=True, name="Reddit-Fetch")
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT", REDDIT_BATCH_GAP_SECONDS,
              REDDIT_BATCH_TIMEOUT_SECONDS, REDDIT_SEARCH_KEYWORDS),
        daemon=True, name="Reddit-Batch",
    )
    serp_thread.start()
    fetch_thread.start()
    btch_thread.start()
    log.info(f"Reddit threads running: SERP-Discovery ✅ | Reddit-Fetch ✅ | Batch ✅ | "
             f"gap:{REDDIT_BATCH_GAP_SECONDS}s | timeout:{REDDIT_BATCH_TIMEOUT_SECONDS}s")

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
    title="Flintel v9.13 — Reddit (SERP discovery decoupled from Reddit fetch via flintel_google_posts + Python auto-fuzzy keyword filtering) + Twitter Signal Scorer",
    description=(
        "Reddit SERP discovery now saves every result into a "
        "flintel_google_posts collection (post_url + google_rank + the exact "
        "search_keyword used + subreddit + Python auto-generated "
        "fuzzy_keywords) the instant it's found — Google SERP storage never "
        "waits on Reddit. A fully separate Reddit-fetch loop reads that same "
        "collection directly (no parallel python list of subreddits/keywords "
        "anywhere), fetches each due post's public per-post RSS feed "
        "(credential-free, smart-retry + old.reddit.com fallback, no OAuth/"
        "PRAW, no .json endpoint anywhere), filters the fetched content "
        "against that post's own stored fuzzy keywords, and — on a match — "
        "reads search_volume from the flintel_keywords cache, builds the "
        "exact same item schema as before, and queues it for LLM scoring "
        "exactly as always. v9.13: scoring now goes through the RapidAPI "
        "GPT-5 /ask endpoint instead of the Anthropic Claude API, and Google "
        "SERP/rank calls now use a dedicated GOOGLE_RAPIDAPI_KEY separate "
        "from the search-volume RAPIDAPI_KEY."
    ),
    version="9.13.0",
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
    pending_reddit_fetch = db.flintel_google_posts.count_documents({"reddit_fetched": False})
    fetched_reddit_posts = db.flintel_google_posts.count_documents({"reddit_fetched": True})
    fuzzy_matched_posts  = db.flintel_google_posts.count_documents({"fuzzy_matched": True})
    fuzzy_no_match_posts = db.flintel_google_posts.count_documents({"fuzzy_matched": False})

    return {
        "status":                  "running",
        "system":                  "FLINTEL v9.13 (Reddit SERP-discovery/fetch decoupled via flintel_google_posts + auto-fuzzy keywords + Twitter; scoring via RapidAPI GPT-5; Google SERP calls on a dedicated RapidAPI key)",
        "client":                  CLIENT_ID,
        "platforms":               ["reddit", "twitter"],
        "reddit_enabled":          REDDIT_ENABLED,
        "reddit_status":           _working(REDDIT_ENABLED and bool(GOOGLE_RAPIDAPI_KEY)),
        "reddit_fetch_method":     "public per-post RSS (credential-free, smart-retry + old.reddit.com fallback) — no OAuth/PRAW, no .json endpoint anywhere",
        "twitter_enabled":         TWITTER_ENABLED,
        "twitter_status":          _working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN)),
        "reddit_search_keywords":  len(REDDIT_SEARCH_KEYWORDS),
        "twitter_search_keywords": len(TWITTER_SEARCH_KEYWORDS),
        "keyword_check_interval_seconds": KEYWORD_CHECK_INTERVAL_SECONDS,
        "keyword_cache":                  "ENABLED — fetch-once-forever, restart-safe (flintel_keywords) — UNTOUCHED from v9.11.1",
        "search_volume_seeding":           f"BATCHED loop (chunks of {SEARCH_VOLUME_BATCH_SIZE}) — UNTOUCHED, uses RAPIDAPI_KEY",
        "search_volume_random_fallback":   f"ENABLED — range {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-{SEARCH_VOLUME_RANDOM_FALLBACK_MAX} — UNTOUCHED",
        "google_serp_rank_key":            "GOOGLE_RAPIDAPI_KEY (dedicated, v9.13) — separate from search-volume RAPIDAPI_KEY",
        "scoring_provider":                "RapidAPI GPT-5 (chatgpt-gpt5.p.rapidapi.com/ask) — v9.13, replaces Anthropic Claude API",
        "reddit_serp_reddit_fetch_decoupled": True,
        "reddit_batch_redundant_filter_fixed": True,
        "batch_processor_item_loss_window_fixed": True,
        "batch_processor_short_text_drop_logged": True,
        "google_posts_collection":        "flintel_google_posts",
        "google_posts_tracked":           total_google_posts,
        "google_posts_pending_reddit_fetch": pending_reddit_fetch,
        "google_posts_reddit_fetched":    fetched_reddit_posts,
        "google_posts_fuzzy_matched":     fuzzy_matched_posts,
        "google_posts_fuzzy_no_match":    fuzzy_no_match_posts,
        "reddit_fetch_check_interval_seconds": REDDIT_FETCH_CHECK_INTERVAL_SECONDS,
        "reddit_post_retry_cooldown_seconds":  REDDIT_POST_RETRY_COOLDOWN_SECONDS,
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
        "rapidapi_search_volume_configured": bool(RAPIDAPI_KEY),
        "rapidapi_google_serp_configured":   bool(GOOGLE_RAPIDAPI_KEY),
        "rapidapi_chatgpt_configured":       bool(CHATGPT_RAPIDAPI_KEY),
        "reddit_queue_size":       reddit_queue.qsize(),
        "twitter_queue_size":      twitter_queue.qsize(),
        "rescore_pending":         db.signals.count_documents({"status": "pending"}),
        "auth_required":           bool(API_KEY),
        "telegram_removed":        True,
        "reddit_json_endpoint_removed": True,
        "reddit_oauth_praw_removed": True,
        "fixed_full_cycle_sleep_removed": True,
        "post_url_dedup_before_scoring": True,
        "claude_failure_routes_to_pending": True,
        "keyword_due_state_independent_of_python_list": True,
        "flintel_keywords_untouched": True,
        "google_rank_serp_logic_untouched_except_key": True,
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
        "reddit_working":          REDDIT_ENABLED and bool(GOOGLE_RAPIDAPI_KEY),
        "reddit_indicator":        _working(REDDIT_ENABLED and bool(GOOGLE_RAPIDAPI_KEY)),
        "reddit_fetch_method":     "public per-post RSS (credential-free) — no OAuth/PRAW",
        "reddit_serp_reddit_fetch_decoupled": True,
        "twitter_working":         TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN),
        "twitter_indicator":       _working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN)),
        "scoring_provider":        "RapidAPI GPT-5",
        "scoring_configured":      bool(CHATGPT_RAPIDAPI_KEY),
        "reddit_queue_size":       reddit_queue.qsize(),
        "twitter_queue_size":      twitter_queue.qsize(),
        "google_posts_pending_reddit_fetch": db.flintel_google_posts.count_documents({"reddit_fetched": False}),
        "rescore_pending":         db.signals.count_documents({"status": "pending"}),
        "client_id":               CLIENT_ID,
        "timestamp":               datetime.now(timezone.utc).isoformat(),
    }


@app.get("/keywords", dependencies=[Depends(verify_api_key)])
def get_keywords_status():
    """UNCHANGED from v9.11.1 — inspects the untouched flintel_keywords
    fetch-once-forever cache directly."""
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
def get_google_posts_status(reddit_fetched: bool = None, fuzzy_matched: bool = None, limit: int = 200):
    """
    Inspect the flintel_google_posts collection directly: every Reddit
    post_url SERP discovery has ever found, its google_rank, the
    search_keyword + auto-generated fuzzy_keywords it was discovered
    under, its subreddit, whether it's been Reddit-fetched yet
    (reddit_fetched), and — once fetched — whether its content actually
    matched the fuzzy keywords (fuzzy_matched: true/false/null).
    """
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
    log.info("  FLINTEL v9.13 — REDDIT SERP-DISCOVERY / REDDIT-FETCH DECOUPLED")
    log.info("                   VIA flintel_google_posts COLLECTION +")
    log.info("                   PYTHON AUTO-FUZZY KEYWORD GENERATION/FILTERING")
    log.info("                   + TWITTER SIGNAL SCORER")
    log.info("                   (+ scoring provider swapped: Claude -> RapidAPI GPT-5)")
    log.info("                   (+ Google SERP/rank calls on a dedicated RapidAPI key)")
    log.info("=" * 70)
    log.info(f"  Client                : {CLIENT_ID}")
    log.info(f"  Platforms             : Reddit (SERP discovery + separate fetch loop) + Twitter/X")
    log.info(f"  Reddit                : {REDDIT_ENABLED} | {_working(REDDIT_ENABLED and bool(GOOGLE_RAPIDAPI_KEY))}")
    log.info(f"  Reddit fetch method   : public per-post RSS only — credential-free, no OAuth/PRAW, no .json anywhere")
    log.info(f"  Reddit engagement     : RANDOM placeholder {REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN}-{REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX} (upvotes/comments) — RSS has no real counts, always logged")
    log.info(f"  Twitter               : {TWITTER_ENABLED} | {_working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN))}")
    log.info(f"  Reddit keywords       : {len(REDDIT_SEARCH_KEYWORDS)} (used ONLY to seed brand-new flintel_keywords docs)")
    log.info(f"  Twitter keywords      : {len(TWITTER_SEARCH_KEYWORDS)} (used for Twitter search query)")
    log.info(f"  Keyword cache         : flintel_keywords — fetch-once-forever, UNTOUCHED from v9.11.1")
    log.info(f"  Google SERP / rank    : search_google_for_keyword() / fetch_google_rank() — UNCHANGED logic, now authenticate with dedicated GOOGLE_RAPIDAPI_KEY (v9.13)")
    log.info(f"  Search-volume         : fetch_search_volume() / seed_search_volume_batch() — UNTOUCHED, still on RAPIDAPI_KEY")
    log.info(f"  Google-posts coll.    : flintel_google_posts — stores post_url + google_rank + search_keyword + subreddit + auto fuzzy_keywords + reddit_fetched")
    log.info(f"  SERP -> Google-posts  : every SERP result saved immediately, does NOT wait on Reddit fetch to complete")
    log.info(f"  Reddit fetch loop     : fully separate thread, reads flintel_google_posts directly (no python list of subreddits/keywords/fuzzy-keywords kept anywhere)")
    log.info(f"  Reddit fetch interval : check every {REDDIT_FETCH_CHECK_INTERVAL_SECONDS}s | retry cooldown {REDDIT_POST_RETRY_COOLDOWN_SECONDS}s on genuine fetch failure")
    log.info(f"  Fuzzy keywords        : Python auto-generated per SERP result at save time (generate_fuzzy_keywords()) — stored on the post's own document, used to filter fetched RSS content (passes_fuzzy_filter())")
    log.info(f"  Scoring provider      : RapidAPI GPT-5 ({CHATGPT_RAPIDAPI_URL}) — replaces Anthropic Claude API (v9.13) | configured:{bool(CHATGPT_RAPIDAPI_KEY)}")
    log.info(f"  Batch processor fix   : Reddit items no longer re-filtered by passes_keyword_filter() against the full keyword-phrase list — fuzzy match upstream is the sole gate for Reddit; Twitter unaffected")
    log.info(f"  Batch processor fix 2 : remove_queue_message() moved to AFTER an item's fate is persisted (batch save or logged drop) — closes item-loss window between dequeue and persist; short-text drops now logged + counted")
    log.info(f"  Reddit batch          : {REDDIT_BATCH_SIZE} items OR {REDDIT_BATCH_TIMEOUT_SECONDS}s | gap {REDDIT_BATCH_GAP_SECONDS}s")
    log.info(f"  Twitter batch         : {TWITTER_BATCH_SIZE} items OR {TWITTER_BATCH_TIMEOUT_SECONDS}s | gap {TWITTER_BATCH_GAP_SECONDS}s")
    log.info(f"  Rescore batch         : {RESCORE_BATCH_SIZE} items | poll {RESCORE_POLL_INTERVAL}s | gap {RESCORE_BATCH_GAP_SECONDS}s")
    log.info(f"  RapidAPI search-volume key : {bool(RAPIDAPI_KEY)} (seo-keyword-research host, UNTOUCHED)")
    log.info(f"  RapidAPI Google SERP key   : {bool(GOOGLE_RAPIDAPI_KEY)} (google-search116 host, DEDICATED key, v9.13)")
    log.info(f"  RapidAPI ChatGPT-5 key     : {bool(CHATGPT_RAPIDAPI_KEY)} (chatgpt-gpt5 host, scoring, v9.13)")
    log.info(f"  Telegram              : REMOVED")
    log.info(f"  Reddit .json endpoint : REMOVED (never used — RSS only)")
    log.info(f"  Reddit OAuth/PRAW     : REMOVED")
    log.info(f"  MongoDB DB            : {MONGODB_DB}")
    log.info(f"  API auth              : {'True | ' + _working(True) if API_KEY else 'False | ' + _working(False)}")
    log.info("=" * 70)

    asyncio.run(main())
