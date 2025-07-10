from datetime import datetime, timedelta, timezone
import requests
import certifi
import time
import os
from dotenv import load_dotenv
from utils.db_helpers import insert_facebook_post

load_dotenv()

API_TOKEN      = os.getenv("APIFY_API_TOKEN")
ACTOR_ID       = os.getenv("APIFY_FACEBOOK_ACTOR", "alien_force~facebook-scraper-pro")
BASE_URL       = "https://api.apify.com/v2/acts"
MAX_POSTS      = int(os.getenv("FB_MAX_POSTS", "100"))
CATCHUP_MONTHS = 4

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type":  "application/json"
}

def _run_actor(page_url, facebook_username, since_iso):
    endpoint = f"{BASE_URL}/{ACTOR_ID}/runs?token={API_TOKEN}"
    
    # Dynamically set the 'since' parameter for date filtering based on the scrape type
    if since_iso:
        # If a specific 'since' date is provided (e.g., from last_fetched)
        since = since_iso
    else:
        # For catchup scrape, calculate from 4 months ago
        since = (datetime.now(timezone.utc) - timedelta(days=30*CATCHUP_MONTHS)).isoformat()
    
    payload = {
        "startUrls": [{"url": page_url}],
        "results_limit": MAX_POSTS,  # Limit number of posts
        "since": since,  # Use dynamic date filtering
        "keyword": facebook_username,  # Set facebook_username as the keyword
        "filter_by_recent_posts": False,  # Optional: Whether to filter by recent posts
        "min_wait_time_in_sec": 1,  # Optional: Minimum wait time between requests
        "max_wait_time_in_sec": 4,  # Optional: Maximum wait time between requests
        "cookies": []  # Optional: Add any necessary cookies
    }

    resp = requests.post(endpoint, headers=HEADERS, json=payload)
    resp.raise_for_status()
    return resp.json()["data"]["id"]

def _fetch_results(run_id):
    status_url = f"{BASE_URL}/{ACTOR_ID}/runs/{run_id}?token={API_TOKEN}"
    while True:
        r = requests.get(status_url, headers=HEADERS)
        r.raise_for_status()
        data = r.json()["data"]
        if data["status"] == "SUCCEEDED":
            break
        if data["status"] == "FAILED":
            raise RuntimeError(f"Actor run {run_id} failed")
        time.sleep(2)

    ds = data["defaultDatasetId"]
    items_url = f"https://api.apify.com/v2/datasets/{ds}/items?token={API_TOKEN}"
    items = requests.get(items_url, headers=HEADERS)
    items.raise_for_status()
    return items.json()

def fetch_facebook_for_user(company_name: str,
                            facebook_username: str,
                            since_dt: datetime = None) -> int:
    """
    - company_name: your Users.company_name
    - facebook_username: Users.facebook_username
    - since_dt: datetime of last_fetched_facebook; if None, defaults to now - CATCHUP_MONTHS
    """
    if since_dt:
        since_iso = since_dt.astimezone(timezone.utc).isoformat()
    else:
        # For catchup scraping (last 4 months)
        since_iso = (datetime.now(timezone.utc) - timedelta(days=30*CATCHUP_MONTHS)).isoformat()

    page_url = f"https://www.facebook.com/{facebook_username}"
    run_id   = _run_actor(page_url, facebook_username, since_iso)
    posts    = _fetch_results(run_id)

    count = 0
    for itm in posts:
        post = {
          "post_id":           itm.get("id"),
          "company_name":      company_name,
          "facebook_username": facebook_username,
          "message":           itm.get("message"),
          "created_at":        itm.get("createdAt"),
          "reactions_count":   itm.get("reactionsCount"),
          "comments_count":    itm.get("commentsCount"),
          "share_count":       itm.get("sharedCount"),
          "post_url":          itm.get("postUrl"),
        }
        if not post["post_id"] or not post["created_at"]:
            continue
        insert_facebook_post(post)
        count += 1

    print(f"[FB] inserted {count} posts for {facebook_username}")
    return count
