import os, time, requests
from datetime import datetime, timedelta, timezone
from utils.db_helpers import insert_facebook_post

API_TOKEN    = os.getenv("APIFY_API_TOKEN")
ACTOR_ID     = os.getenv("APIFY_FACEBOOK_ACTOR", "apify/facebook-posts-scraper")
ACTOR_BASE   = "https://api.apify.com/v2/acts"
DATASET_BASE = "https://api.apify.com/v2/datasets"

MAX_POSTS    = int(os.getenv("FB_MAX_POSTS",     "100"))
CATCHUP_MOS  = int(os.getenv("FB_CATCHUP_MONTHS", "3"))

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type":  "application/json",
}


def _run_actor(page_url: str, since_iso: str | None) -> str:
    actor_path = ACTOR_ID.replace("/", "~")
    endpoint   = f"{ACTOR_BASE}/{actor_path}/runs?token={API_TOKEN}"

    payload = {
      "startUrls":    [{"url": page_url}],
      "resultsLimit": MAX_POSTS,
      "maxRequestRetries": 3,
      # disable Apify Proxy if you don’t have credit:
      "proxy": {
    "useApifyProxy": True,
    "apifyProxyGroups": ["RESIDENTIAL"]  # optional
  },
      # only include "since" when you want it
      **({"since": since_iso} if since_iso else {})
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
            raise RuntimeError(f"Actor run {run_id} failed: {data.get('error')}")
        time.sleep(2)

    ds         = data["defaultDatasetId"]
    items_url  = f"{DATASET_BASE}/{ds}/items?token={API_TOKEN}"
    items_resp = requests.get(items_url, headers=HEADERS)
    items_resp.raise_for_status()
    return items_resp.json()

def fetch_facebook_for_user(
    company_name: str,
    facebook_username: str,
    since_dt: datetime | None = None
) -> int:
    # 1) Build the ISO or cutoff
    if since_dt:
        since_iso = since_dt.astimezone(timezone.utc).isoformat()
    else:
        cutoff    = datetime.now(timezone.utc) - timedelta(days=30*CATCHUP_MOS)
        since_iso = cutoff.isoformat()

    # 2) Kick off the actor
    page_url = f"https://www.facebook.com/{facebook_username}"
    run_id   = _run_actor(page_url, since_iso)

    # 3) Poll until it's done and pull the JSON
    posts = _fetch_results(run_id)

    # 4) Insert each post
    count = 0
    for itm in posts:
        # — unwrap the very first mention in textReferences (if any)
        author_name = None
        text_refs = itm.get("textReferences") or []
        if isinstance(text_refs, list) and text_refs:
            tr = text_refs[0]
            author_name = tr.get("short_name") or tr.get("shortname")

        # — unwrap the very first image in media (if any)
        image_url = None
        media_list = itm.get("media") or []
        for m in media_list:
            if isinstance(m, dict) and m.get("photo_image"):
                image_url = m["photo_image"].get("url")
                break
            if isinstance(m, dict) and m.get("image"):
                image_url = m["image"].get("uri") or m["image"].get("url")
                break

        post = {
            "post_id":           itm.get("postFacebookId") or itm.get("postId"),
            "company_name":      company_name,
            "facebook_username": facebook_username,
            "author_name":       author_name,
            "message":           itm.get("text"),
            "created_at":        itm.get("time"),
            "reactions_count":   itm.get("likes"),
            "comments_count":    itm.get("comments"),
            "share_count":       itm.get("shares"),
            "image":             image_url,
            "post_url":          itm.get("url"),
        }

        # skip if no ID or time
        if not post["post_id"] or not post["created_at"]:
            continue

        insert_facebook_post(post)
        count += 1

    print(f"[FB] Inserted {count} posts for {facebook_username}")
    return count
