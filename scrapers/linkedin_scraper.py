# linkedin_scraper.py

import os, time, requests
from datetime import datetime, timezone
from utils.db_helpers import insert_linkedin_post
import logging

API_TOKEN        = os.getenv("APIFY_API_TOKEN")
ACTOR_ID         = os.getenv("APIFY_LINKEDIN_ACTOR", "apimaestro/linkedin-profile-posts")
ACTOR_BASE       = "https://api.apify.com/v2/acts"
DATASET_BASE     = "https://api.apify.com/v2/datasets"
LI_GENERAL_LIMIT = int(os.getenv("LI_GENERAL_LIMIT", "20"))
LI_CATCHUP_LIMIT = int(os.getenv("LI_CATCHUP_LIMIT",  "10"))

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type":  "application/json",
}
logger = logging.getLogger(__name__)


def _run_actor(username: str, limit: int, page_number: int = 1) -> str:
    actor_path = ACTOR_ID.replace("/", "~")
    endpoint   = f"{ACTOR_BASE}/{actor_path}/runs?token={API_TOKEN}"
    payload = {
        "username":    username,
        "page_number": page_number,
        "limit":       limit,
    }
    resp = requests.post(endpoint, headers=HEADERS, json=payload)
    resp.raise_for_status()
    return resp.json()["data"]["id"]


def _fetch_results(run_id: str) -> list[dict]:
    status_url = f"{ACTOR_BASE}/{ACTOR_ID.replace('/', '~')}/runs/{run_id}?token={API_TOKEN}"
    while True:
        r = requests.get(status_url, headers=HEADERS)
        r.raise_for_status()
        data = r.json()["data"]
        if data["status"] == "SUCCEEDED":
            break
        if data["status"] == "FAILED":
            raise RuntimeError(f"LinkedIn actor failed: {data.get('error')}")
        time.sleep(2)

    ds         = data["defaultDatasetId"]
    items_url  = f"{DATASET_BASE}/{ds}/items?token={API_TOKEN}"
    items_resp = requests.get(items_url, headers=HEADERS)
    items_resp.raise_for_status()
    return items_resp.json()


def fetch_linkedin_posts_for_user(
    company_name: str,
    linkedin_username: str,
    since_dt: datetime | None = None
) -> int:
    # decide limit: general vs catchup
    limit  = LI_GENERAL_LIMIT if since_dt else LI_CATCHUP_LIMIT
    run_id = _run_actor(linkedin_username, limit)
    posts  = _fetch_results(run_id)

    inserted = 0
    for itm in posts:
        # 1) guard against missing posted_at entirely
        pa = itm.get("posted_at")
        if not isinstance(pa, dict):
            logger.warning(f"[LI] skip {itm.get('urn')}—no posted_at")
            continue

        # 2) grab both date-string & timestamp
        date_str = pa.get("date")          # e.g. "2025-04-15 22:28:47"
        ts_ms    = pa.get("timestamp")     # e.g. 1744748927743
        if date_str is None or ts_ms is None:
            logger.warning(f"[LI] skip {itm.get('urn')}—incomplete posted_at")
            continue

        # 3) build ISO timestamp
        iso = datetime.fromtimestamp(ts_ms/1000, timezone.utc).isoformat()

        # 4) pull stats
        stats = itm.get("stats", {})
        post = {
            "company_name":      company_name,
            "urn":               itm.get("full_urn") or itm.get("urn"),
            "text":              itm.get("text"),
            "url":               itm.get("url"),
            "posted_at_iso":     iso,
            "posted_at_ts":      ts_ms,
            "author_name":       " ".join(filter(None, (
                                      itm.get("author",{}).get("first_name"),
                                      itm.get("author",{}).get("last_name"),
                                   ))),
            "author_profile_id": itm.get("author",{}).get("username"),
            "author_headline":   itm.get("author",{}).get("headline"),
            "image":             itm.get("media", {}).get("url"),
            # ─── reaction fields ───
            "total_reactions":   stats.get("total_reactions", 0),
            "like_count":        stats.get("like",            0),
            "support":           stats.get("support",         0),
            "love":              stats.get("love",            0),
            "insight":           stats.get("insight",         0),
            "celebrate":         stats.get("celebrate",       0),
            "comments_count":    stats.get("comments",        0),
            "reposts":           stats.get("reposts",         0),
            "type":              itm.get("post_type"),
           # "raw":               itm,  # save full JSON for debugging
        }

        insert_linkedin_post(post)
        inserted += 1

    return inserted
