# scrapers/reddit_scraper.py
import random
import time
import os
import logging
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from fake_useragent import UserAgent

from utils.db_helpers import insert_reddit_post

load_dotenv()
logger      = logging.getLogger(__name__)
MAX_POSTS   = int(os.getenv("REDDIT_MAX_POSTS", "30"))
SLEEP_BETWN = (1.0, 2.5)

# ---------- rotating User-Agent ----------
try:
    _ua = UserAgent()
except Exception as e:
    logger.warning(f"fake_useragent init failed; static UA fallback: {e}")
    _ua = None

def _headers() -> dict:
    try:
        ua_str = _ua.random if _ua else None
    except Exception as e:
        logger.warning(f"fake_useragent error: {e}")
        ua_str = None

    if not ua_str:
        ua_str = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        )

    return {
        "User-Agent":      ua_str,
        "Accept":          "application/json",
        "Accept-Language": "en-US,en;q=0.9",
    }

# ---------- main scraper ----------
SEARCH_API = "https://www.reddit.com/search.json"

def _fetch_page(query: str, after: str | None):
    params = {"q": query, "limit": 25, "sort": "new"}
    if after:
        params["after"] = after
    resp = requests.get(SEARCH_API, headers=_headers(), params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json().get("data", {})
    return data.get("children", []), data.get("after")

def fetch_reddit_for_company(company_name: str, company_web: str) -> int:
    """
    Scrape Reddit for both:
      • posts linking to the domain   (url:<domain>)
      • posts mentioning the domain   (<domain>)
    Inserts up to MAX_POSTS rows into reddit_posts, now including review_date.
    """
    inserted_total = 0

    for query in (f"url:{company_web}", company_web):
        after = None
        while inserted_total < MAX_POSTS:
            try:
                children, after = _fetch_page(query, after)
            except requests.RequestException as e:
                logger.warning(f"[Reddit][{query}] request failed: {e}")
                break

            if not children:
                break

            for child in children:
                if inserted_total >= MAX_POSTS:
                    break
                post = child.get("data", {})

                # --- New: extract created_utc → ISO timestamp ---
                created_ts = post.get("created_utc")
                if created_ts is not None:
                    review_date = datetime.fromtimestamp(created_ts, timezone.utc).isoformat()
                else:
                    review_date = None

                post_url   = "https://www.reddit.com" + post.get("permalink", "")
                title      = post.get("title") or ""
                author     = post.get("author")
                votes      = post.get("score", 0)
                comments   = post.get("num_comments", 0)
                image_url  = None
                preview    = post.get("preview", {}).get("images", [])
                if preview:
                    image_url = preview[0].get("source", {}).get("url")
                full_text  = post.get("selftext") or None

                try:
                    insert_reddit_post({
                        "company_name": company_name,
                        "post_url":     post_url,
                        "title":        title,
                        "author":       author,
                        "image_url":    image_url,
                        "votes":        votes,
                        "comments":     comments,
                        "full_review":  full_text,
                        "review_date":  review_date,    # ← new field
                    })
                    inserted_total += 1
                except Exception as e:
                    logger.error(f"[Reddit][DB] insert error: {e}")

            if not after:
                break
            time.sleep(random.uniform(*SLEEP_BETWN))

        if inserted_total >= MAX_POSTS:
            break

    logger.info(f"[Reddit][{company_web}] inserted {inserted_total} posts.")
    return inserted_total
