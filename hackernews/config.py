"""
hackernews/config.py
--------------------
Hacker News service ki saari settings. Is file mein koi network / DB call nahi hai.

Do hisse hain:
  1. Live-checked switches: har call par .env dobara load hota hai (override=True), taake
     service restart kiye baghair switch badla ja sake.
  2. Module-level constants: import ke waqt ek dafa os.getenv se padhe jate hain.

NOTE: index.py se kuch import NAHI kiya jata (circular import se bachne ke liye), isliye
`_env_bool` ka apna chhota helper yahan maujood hai.
"""

import os

from dotenv import load_dotenv


def _env_bool(name: str, default: bool) -> bool:
    """
    Environment variable ko bool mein parse karta hai.
    Accepted (case-insensitive): true/1/yes/on  aur  false/0/no/off.
    Variable set na ho ya value samajh na aaye to `default` return hota hai.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in ("true", "1", "yes", "on"):
        return True
    if value in ("false", "0", "no", "off"):
        return False
    return default


# ---------------------------------------------------------------------------
# Live-checked switches (har call par .env dobara load hota hai)
# ---------------------------------------------------------------------------

def is_hackernews_enabled() -> bool:
    """Master switch: poori Hacker News service on/off. Default: True."""
    load_dotenv(override=True)
    return _env_bool("HACKERNEWS_ENABLED", True)


def is_hn_stories_enabled() -> bool:
    """Hacker News stories fetch karni hain ya nahi. Default: True."""
    load_dotenv(override=True)
    return _env_bool("HN_FETCH_STORIES", True)


def is_hn_comments_enabled() -> bool:
    """Hacker News comments fetch karne hain ya nahi. Default: True."""
    load_dotenv(override=True)
    return _env_bool("HN_FETCH_COMMENTS", True)


def is_hn_strict_match_enabled() -> bool:
    """Strict keyword match (text mein keyword lazmi maujood ho) on/off. Default: True."""
    load_dotenv(override=True)
    return _env_bool("HN_STRICT_MATCH", True)


# ---------------------------------------------------------------------------
# Constants (os.getenv, defaults ke saath)
# ---------------------------------------------------------------------------

# Hacker News Algolia API ka endpoint (date ke hisaab se sorted, free, key nahi chahiye).
HN_API_URL = "https://hn.algolia.com/api/v1/search_by_date"

# Algolia ka hitsPerPage: ek request mein kitne results mange jayein (clamp 1..1000).
HN_RESULTS_PER_QUERY = max(1, min(1000, int(os.getenv("HN_RESULTS_PER_QUERY", "50"))))

# Ek keyword + kind (story/comment) ke liye maximum kitne pages fetch hon (clamp >= 1).
HN_MAX_PAGES = max(1, int(os.getenv("HN_MAX_PAGES", "5")))

# Har HTTP request ka timeout (seconds).
HN_REQUEST_TIMEOUT = int(os.getenv("HN_REQUEST_TIMEOUT", "15"))

# Do keywords ke darmiyan wait (seconds), taake API par bohat zyada load na pare.
HN_KEYWORD_GAP_SECONDS = float(os.getenv("HN_KEYWORD_GAP_SECONDS", "1.0"))

# Do poore cycles ke darmiyan wait (seconds).
HN_POLL_INTERVAL_SECONDS = int(os.getenv("HN_POLL_INTERVAL_SECONDS", "600"))

# Pehli dafa (jab koi saved state na ho) kitne din peeche tak ka data uthana hai.
# Default wahi LOOKBACK_DAYS jo baqi service use karti hai.
HN_LOOKBACK_DAYS = int(os.getenv("HN_LOOKBACK_DAYS", os.getenv("LOOKBACK_DAYS", "180")))

# True ho to multi-word keyword exact phrase ke tor par (quotes mein) bheja jaye.
HN_EXACT_PHRASE = _env_bool("HN_EXACT_PHRASE", False)

# HTTP requests ke saath bheja jane wala User-Agent.
HN_USER_AGENT = os.getenv(
    "HN_USER_AGENT", "flintel-fetch-service/1.0 (contact: ops@example.com)"
)

# flintel_signals mein save hone wale platform ke naam.
HN_PLATFORM_STORY = "hackernews"
HN_PLATFORM_COMMENT = "hackernews_comment"

# MongoDB collection jis mein har keyword + kind ka last-fetched state rakha jayega.
HN_STATE_COLLECTION = "flintel_hn_state"
