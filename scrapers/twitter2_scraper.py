# scrapers/twitter2_scraper.py

import os
import requests
from datetime import timezone
from dotenv import load_dotenv
from utils.db_helpers import insert_twitter_mention

load_dotenv()

API_TOKEN = os.getenv("APIFY_API_TOKEN")
ACTOR_ID  = os.getenv("APIFY_TWITTER_SCRAPER_ACTOR", "apidojo~twitter-scraper-lite")
BASE_URL  = "https://api.apify.com/v2/acts"

if not API_TOKEN:
    raise RuntimeError("APIFY_API_TOKEN not set in environment")

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type":  "application/json"
}

def fetch_tweets_for_user(company_name, twitter_username,
                          last_fetched=None, max_items=10):
   
    endpoint = (
        f"{BASE_URL}/{ACTOR_ID}"
        "/run-sync-get-dataset-items"
        f"?token={API_TOKEN}"
    )
    payload = {
        "author":   twitter_username,
        "maxItems": max_items,
    }
    if last_fetched:
        payload["start"] = last_fetched.astimezone(timezone.utc).isoformat()

    resp = requests.post(endpoint, headers=HEADERS, json=payload)
    resp.raise_for_status()
    items = resp.json()

    # If free plan blocks API usage, Apify returns an empty list or status messages—
    # in which case no real tweets exist
    if not isinstance(items, list) or not items:
        print(f"[WARN] No tweets returned for @{twitter_username} (free‐plan limit?)")
        return 0

    count = 0
    for item in items:
        tweet_id = item.get("id")
        text     = item.get("text")

        # Skip any entry missing the two required fields
        if not tweet_id or not text:
            continue

        tweet = {
            "tweet_id":     tweet_id,
            "company_name": company_name,
            "text":         text,
            "author_handle": twitter_username,
            "created_at":   item.get("createdAt"),
            "reply_count":  item.get("replyCount", 0),
        }
        insert_twitter_mention(tweet)
        count += 1

    print(f"[INFO] Inserted {count} tweets for @{twitter_username}")
    return count
