"""
hackernews/database.py
----------------------
Hacker News service ka MongoDB #2 (db2) se rabta.

Is file ke sirf teen kaam hain:
  1. db2 ka lazy, thread-safe MongoClient dena (`get_db2`).
  2. `flintel_keywords` se keywords padhna (`load_hn_keywords`).
  3. Incremental state (`flintel_hn_state`) padhna / likhna
     (`get_last_seen_ts` / `set_last_seen_ts`).

`flintel_signals` mein save karna is file ka kaam NAHI hai (wo index.py ka `save_signal`
karta hai). Existing collections (flintel_keywords, flintel_google_posts) mein koi write ya
index change nahi hota. Sirf naya `flintel_hn_state` use hota hai.

Koi function raise nahi karta; har failure par log.warning karke None / [] return hota hai.
"""

import logging
import os
import threading

from pymongo import ASCENDING, MongoClient

from . import config

log = logging.getLogger("flintel")

# Lazy client + uska lock. Client pehli call par banta hai, phir reuse hota hai.
_client = None
_client_lock = threading.Lock()

# `flintel_hn_state` ka unique index sirf ek dafa ensure kiya jata hai.
_state_index_ready = False
_state_index_lock = threading.Lock()


def get_db2():
    """
    MongoDB #2 ka database object return karta hai (ya None agar connect/setup fail ho).

    URI : MONGODB2_URI, na ho to MONGODB_URI.
    DB  : MONGODB2_DB, na ho to MONGODB_DB, na ho to "fx_signals".

    Client import ke waqt nahi banta. Pehli call par lock ke andar banta hai aur baad mein
    reuse hota hai. Agar client ban na paye to cache nahi hota, taake agli call dobara try kare.
    """
    global _client

    uri = os.getenv("MONGODB2_URI") or os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB2_DB") or os.getenv("MONGODB_DB") or "fx_signals"

    if not uri:
        log.warning("[HACKERNEWS] MongoDB #2 URI set nahi hai (MONGODB2_URI / MONGODB_URI).")
        return None

    try:
        with _client_lock:
            if _client is None:
                _client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client = _client
        return client[db_name]
    except Exception as exc:
        log.warning("[HACKERNEWS] MongoDB #2 client banane mein masla: %s", exc)
        return None


def load_hn_keywords() -> list:
    """
    db2.flintel_keywords se SAARE keyword docs padhta hai (`fetched` flag ignore hota hai).

    Sirf field "keyword" li jati hai. Strip hota hai, khali values hatai jati hain, aur
    duplicates (case-insensitive) hatay jate hain; pehli dafa jo order mila wohi rehta hai.
    Error par [] return hota hai.
    """
    try:
        db = get_db2()
        if db is None:
            return []

        keywords = []
        seen = set()
        for doc in db["flintel_keywords"].find({}, {"keyword": 1, "_id": 0}):
            raw = doc.get("keyword")
            if raw is None:
                continue
            keyword = str(raw).strip()
            if not keyword:
                continue
            key = keyword.lower()
            if key in seen:
                continue
            seen.add(key)
            keywords.append(keyword)

        log.info("[HACKERNEWS] %d keyword(s) load hue.", len(keywords))
        return keywords
    except Exception as exc:
        log.warning("[HACKERNEWS] Keywords load karne mein masla: %s", exc)
        return []


def _get_state_collection():
    """
    `flintel_hn_state` collection return karta hai (ya None). Pehli dafa (keyword, kind) par
    unique index ensure karta hai; index fail ho to bhi collection use hota rehta hai.
    """
    global _state_index_ready

    db = get_db2()
    if db is None:
        return None

    coll = db[config.HN_STATE_COLLECTION]

    if not _state_index_ready:
        with _state_index_lock:
            if not _state_index_ready:
                try:
                    coll.create_index(
                        [("keyword", ASCENDING), ("kind", ASCENDING)],
                        unique=True,
                        name="keyword_1_kind_1",
                    )
                except Exception as exc:
                    log.warning("[HACKERNEWS] %s ka unique index banane mein masla: %s",
                                config.HN_STATE_COLLECTION, exc)
                # Ek hi dafa try hota hai (success ho ya fail), taake har call par warning na aaye.
                _state_index_ready = True

    return coll


def get_last_seen_ts(keyword: str, kind: str):
    """
    Is keyword + kind ("story" ya "comment") ke liye ab tak ka sab se bara created_at_i
    (unix seconds) return karta hai. Koi state na ho ya error aaye to None.
    """
    try:
        coll = _get_state_collection()
        if coll is None:
            return None
        doc = coll.find_one({"keyword": keyword, "kind": kind}, {"last_created_at_i": 1})
        if not doc:
            return None
        value = doc.get("last_created_at_i")
        return int(value) if value is not None else None
    except Exception as exc:
        log.warning("[HACKERNEWS] last_seen_ts padhne mein masla (%s/%s): %s", keyword, kind, exc)
        return None


def set_last_seen_ts(keyword: str, kind: str, ts: int) -> None:
    """
    Is keyword + kind ka last_created_at_i upsert karta hai. `$max` use hota hai, yani
    sirf bara ts save hota hai (purana ts kabhi wapas nahi aata). Fail par sirf log.warning.
    """
    try:
        coll = _get_state_collection()
        if coll is None:
            return
        coll.update_one(
            {"keyword": keyword, "kind": kind},
            {"$max": {"last_created_at_i": int(ts)}},
            upsert=True,
        )
    except Exception as exc:
        log.warning("[HACKERNEWS] last_seen_ts likhne mein masla (%s/%s): %s", keyword, kind, exc)
