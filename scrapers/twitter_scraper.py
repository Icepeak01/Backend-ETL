# scrapers/twitter_scraper.py
import os, requests
from datetime import datetime, timedelta, timezone
from utils.db_helpers import insert_twitter_mention

# Apify actor configuration
API_TOKEN  = os.getenv("APIFY_API_TOKEN")
ACTOR_ID   = os.getenv(
    "APIFY_TWITTER_SCRAPER_ACTOR",
    "kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest"
)
ACTOR_BASE = "https://api.apify.com/v2/acts"
HEADERS    = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type":  "application/json",
}


def _run_actor(username: str, since_iso: str, until_iso: str) -> list[dict]:
    
    endpoint = (
        f"{ACTOR_BASE}/{ACTOR_ID.replace('/', '~')}"
        "/run-sync-get-dataset-items"
        f"?token={API_TOKEN}"
    )
    # build a mention-or-hashtag query covering our window
    query = f"@{username} OR #{username} since:{since_iso} until:{until_iso}"
    payload = {
        "searchTerms":    [query],
        "lang":           "en",
        # ensure Tweets with @mentions are returned
        "filter:mentions": True,
        "filter:hashtags": True,
        "filter:replies": True,
        "maxItems": 500,

    }
    resp = requests.post(endpoint, headers=HEADERS, json=payload)
    resp.raise_for_status()
    return resp.json()



def fetch_twitter_mentions_for_user(
    company_name:   str,
    twitter_username: str,
    since_dt:       datetime | None = None,
) -> int:
    """
    Scrape *all* tweets mentioning `twitter_username` since `since_dt`
    (or the last 90 days if None), insert into DB, and return the count.
    """
    until = datetime.utcnow().replace(tzinfo=timezone.utc)
    since = (since_dt.astimezone(timezone.utc)
             if since_dt
             else (until - timedelta(days=90)))

    # format for Twitter-X actor
    since_iso = since.strftime("%Y-%m-%d_%H:%M:%S_UTC")
    until_iso = until.strftime("%Y-%m-%d_%H:%M:%S_UTC")

    items = _run_actor(twitter_username, since_iso, until_iso)
    count = 0

    for itm in items:
        raw_ts = itm.get("createdAt")
        if not raw_ts:
            continue
        created = datetime.strptime(raw_ts, "%a %b %d %H:%M:%S %z %Y") \
                          .astimezone(timezone.utc)


        rec = {
            "company_name":  company_name,
            "tweet_id":      itm.get("id"),
            "twitter_url":   itm.get("url"),
            "text":          itm.get("text"),
            "retweet_count": itm.get("retweetCount"),
            "reply_count":   itm.get("replyCount"),
            "like_count":    itm.get("likeCount"),
            "view_count":    itm.get("viewCount"),
            "created_at":    created,
            "author_handle": itm.get("author", {}).get("name"),
            "image": (itm.get("media") or [{}])[0].get("expanded_url"),
            "videourl": None,
        }

        # if there's a video variant, grab its URL
        for m in (itm.get("extendedEntities") or {}).get("media", []):
            if m.get("type") in ("video", "animated_gif"):
                variants = m.get("videoInfo", {}).get("variants", [])
                if variants:
                    rec["videourl"] = variants[0].get("url")
                break

        insert_twitter_mention(rec)
        count += 1

    return count
