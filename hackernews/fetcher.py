"""
hackernews/fetcher.py
---------------------
Hacker News (Algolia) se keyword-based search, index.py ke `item` shape mein conversion,
strict keyword match, pagination aur always-on loop.

Is file se `import index` KABHI nahi hota (circular import). `save_signal` aur sinks check
function argument ke tor par pass hote hain (`save_fn`, `sinks_ok_fn`). `save_fn(item)` ka
signature / behaviour yahan change nahi hota, hum sirf use call karte hain.

Koi function aisa nahi jo raise karke thread crash kare; har failure sirf log hoti hai.
"""

import html
import logging
import re
import time
from datetime import datetime, timedelta, timezone

import requests

from . import config, database

log = logging.getLogger("flintel")

# Strict match mein in lafzon ko ignore kiya jata hai (aur 2 ya us se chhote lafz bhi).
_STOPWORDS = {
    "a", "an", "the", "to", "for", "of", "in", "on", "my", "our", "is", "are", "and", "or",
    "with", "from", "at", "by", "your", "their",
}


def _strip_html(s) -> str:
    """HN ke HTML text (comment_text / story_text) ko saaf plain text mein badalta hai."""
    text = re.sub(r"<[^>]+>", " ", html.unescape(s or ""))
    return re.sub(r"\s+", " ", text).strip()


def _significant_words(keyword: str) -> list:
    """Keyword ke ahem lafz: lowercase, stopwords aur chhote (len<=2) lafz hata kar."""
    words = re.findall(r"[a-z0-9']+", (keyword or "").lower())
    return [w for w in words if w not in _STOPWORDS and len(w) > 2]


def keyword_in_post(keyword: str, item: dict) -> bool:
    """
    Strict match. True tab jab:
      - poora keyword (lowercase) text mein substring ho, YA
      - keyword ke SAARE significant words text mein maujood hon (alag-alag, kisi bhi jagah).
    Significant words khali hon to sirf poore keyword wala check lagta hai.
    """
    text = (item.get("text") or "").lower()
    if (keyword or "").lower() in text:
        return True
    words = _significant_words(keyword)
    if not words:
        return False
    return all(w in text for w in words)


def _to_int(value):
    """created_at_i ko int mein badalta hai; na ho ya kharab ho to None."""
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def fetch_hackernews_page(keyword: str, kind: str, since_ts, page: int):
    """
    Algolia se ek page laata hai aur hits ko index.py ke item shape mein badalta hai.

    kind: "story" ya "comment". since_ts diya ho to sirf us se naye (created_at_i > since_ts).
    Return: (items, nbPages). Kabhi raise nahi karta; error par ([], 0).
    Har item mein extra key "_created_at_i" sirf checkpoint ke liye hai (save_signal ignore karta hai).
    """
    try:
        query = keyword
        if config.HN_EXACT_PHRASE and " " in keyword:
            query = f'"{keyword}"'

        params = {
            "query": query,
            "tags": kind,
            "hitsPerPage": config.HN_RESULTS_PER_QUERY,
            "page": page,
        }
        if since_ts is not None:
            params["numericFilters"] = f"created_at_i>{since_ts}"

        resp = requests.get(
            config.HN_API_URL,
            params=params,
            headers={"User-Agent": config.HN_USER_AGENT},
            timeout=config.HN_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        hits = data.get("hits") or []
        nb_pages = int(data.get("nbPages") or 0)

        is_story = kind == "story"
        items = []
        for hit in hits:
            object_id = hit.get("objectID")
            if not object_id:
                continue

            if is_story:
                title = (hit.get("title") or "").strip()
                story_text = _strip_html(hit.get("story_text"))
                text = f"{title}\n\n{story_text}" if (title and story_text) else (title or story_text)
            else:
                text = _strip_html(hit.get("comment_text"))
            if not text:
                continue

            created_at_i = _to_int(hit.get("created_at_i"))
            posted_at = (
                datetime.fromtimestamp(created_at_i, tz=timezone.utc).isoformat()
                if created_at_i is not None else None
            )

            items.append({
                "message_id": (f"hackernews_{object_id}" if is_story
                               else f"hackernews_comment_{object_id}"),
                "platform": config.HN_PLATFORM_STORY if is_story else config.HN_PLATFORM_COMMENT,
                "text": text,
                "username": hit.get("author") or "unknown",
                "subreddit_or_channel": "",
                "post_url": f"https://news.ycombinator.com/item?id={object_id}",
                "posted_at": posted_at,
                "search_keyword": keyword,
                "google_rank": None,
                "_created_at_i": created_at_i,
            })

        return items, nb_pages
    except Exception as exc:
        log.warning(f"[HACKERNEWS] search failed | keyword='{keyword}' | kind={kind} | {exc}")
        return [], 0


def process_keyword_kind(keyword: str, kind: str, save_fn, default_since_ts) -> int:
    """
    Ek keyword + kind ke liye naye posts laata hai (pagination ke saath), strict match lagata
    hai, `save_fn(item)` se save karta hai aur checkpoint aage badhata hai.

    Checkpoint SAARE fetched items (matched + skipped) ke max created_at_i par jata hai.
    Return: is keyword+kind ke naye saved messages ki ginti. Kabhi raise nahi karta.
    """
    count = 0
    try:
        since_ts = database.get_last_seen_ts(keyword, kind)
        if since_ts is None:
            since_ts = default_since_ts

        strict = config.is_hn_strict_match_enabled()
        skipped = 0
        max_ts = None

        for page in range(config.HN_MAX_PAGES):
            items, nb_pages = fetch_hackernews_page(keyword, kind, since_ts, page)
            if not items:
                break

            for item in items:
                ts = item.get("_created_at_i")
                if ts is not None and (max_ts is None or ts > max_ts):
                    max_ts = ts

                if strict and not keyword_in_post(keyword, item):
                    skipped += 1
                    continue

                if save_fn(item):
                    count += 1

            if page + 1 >= nb_pages:
                break
            time.sleep(0.3)

        if max_ts is not None:
            database.set_last_seen_ts(keyword, kind, max_ts)

        if skipped > 0:
            log.info(f"[HACKERNEWS] strict match skipped {skipped} | keyword='{keyword}' | kind={kind}")
    except Exception as exc:
        log.warning(f"[HACKERNEWS] process failed | keyword='{keyword}' | kind={kind} | {exc}")
    return count


def run_hackernews_cycle(save_fn) -> int:
    """
    Ek poora cycle: db2 se saare keywords, har keyword ke liye enabled kinds (story/comment).
    Return: is cycle mein total naye saved messages.
    """
    keywords = database.load_hn_keywords()
    if not keywords:
        log.info("[HACKERNEWS] CYCLE SKIP | koi keyword nahi mila")
        return 0

    log.info(f"[HACKERNEWS] CYCLE START | keywords={len(keywords)}")

    default_since_ts = int(
        (datetime.now(timezone.utc) - timedelta(days=config.HN_LOOKBACK_DAYS)).timestamp()
    )

    total = 0
    for keyword in keywords:
        kinds = []
        if config.is_hn_stories_enabled():
            kinds.append("story")
        if config.is_hn_comments_enabled():
            kinds.append("comment")

        for kind in kinds:
            total += process_keyword_kind(keyword, kind, save_fn, default_since_ts)

        time.sleep(config.HN_KEYWORD_GAP_SECONDS)

    log.info(f"[HACKERNEWS] CYCLE DONE | keywords={len(keywords)} | new_messages_saved={total}")
    return total


def run_hackernews_loop(save_fn, sinks_ok_fn):
    """
    ALWAYS-ON loop, kabhi exit nahi hota.

    Har iteration: service enabled na ho ya dono sinks band hon to cycle skip (is surat mein
    koi fetch nahi hota, isliye checkpoint aage nahi badhta). Warna ek cycle chalta hai.
    Koi bhi exception loop ko nahi rokti: log.error aur 10 second baad dobara.
    """
    while True:
        try:
            if not config.is_hackernews_enabled():
                log.info("[HACKERNEWS] HACKERNEWS_ENABLED=false: HN cycle skip")
                time.sleep(config.HN_POLL_INTERVAL_SECONDS)
                continue

            if not sinks_ok_fn():
                log.warning("[HACKERNEWS] MONGODB_DATA aur MYSQL_DATA dono false: HN cycle skip")
                time.sleep(config.HN_POLL_INTERVAL_SECONDS)
                continue

            run_hackernews_cycle(save_fn)
            time.sleep(config.HN_POLL_INTERVAL_SECONDS)
        except Exception as exc:
            log.error(f"[HACKERNEWS] loop error: {exc}")
            time.sleep(10)
