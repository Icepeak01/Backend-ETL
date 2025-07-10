import os
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from utils.db_helpers import insert_instagram_mention

load_dotenv()
APIFY_API_TOKEN       = os.getenv("APIFY_API_TOKEN")
APIFY_INSTAGRAM_ACTOR = os.getenv("APIFY_INSTAGRAM_ACTOR")
APIFY_BASE_URL        = "https://api.apify.com/v2"

if not APIFY_API_TOKEN or not APIFY_INSTAGRAM_ACTOR:
    raise RuntimeError("APIFY_API_TOKEN or APIFY_INSTAGRAM_ACTOR not set in environment")

HEADERS = {
    "Authorization": f"Bearer {APIFY_API_TOKEN}",
    "Content-Type":  "application/json",
}


def _run_instagram_actor(instagram_username: str, since_iso: str | None = None) -> str:
    url = f"{APIFY_BASE_URL}/acts/{APIFY_INSTAGRAM_ACTOR}/runs?token={APIFY_API_TOKEN}"
    payload = {
        "addParentData": False,
        "searchType":    "hashtag",
        "resultsType":   "posts",
        "search":        instagram_username,
        **({"since": since_iso} if since_iso else {}),
    }
    resp = requests.post(url, headers=HEADERS, json=payload)
    resp.raise_for_status()
    return resp.json()["data"]["id"]


def _fetch_actor_results(run_id: str) -> list[dict]:
    status_url = f"{APIFY_BASE_URL}/acts/{APIFY_INSTAGRAM_ACTOR}/runs/{run_id}?token={APIFY_API_TOKEN}"
    while True:
        r = requests.get(status_url, headers=HEADERS)
        r.raise_for_status()
        data = r.json()["data"]
        status = data.get("status")
        if status == "SUCCEEDED":
            break
        if status == "FAILED":
            raise RuntimeError(f"Actor run {run_id} failed: {data.get('error')}")
        time.sleep(2)

    ds_id     = data["defaultDatasetId"]
    items_url = f"{APIFY_BASE_URL}/datasets/{ds_id}/items?token={APIFY_API_TOKEN}"
    items     = requests.get(items_url, headers=HEADERS)
    items.raise_for_status()
    return items.json()


def fetch_instagram_for_company(
    company_name: str,
    instagram_username: str,
    since_date: datetime | str | None = None,
):
    """
    Scrape Instagram posts for a given instagram_username hashtag,
    then insert into the DB under the shared company_name.
    """
    # Normalize since_date → ISO string
    if isinstance(since_date, datetime):
        since_iso = since_date.astimezone(timezone.utc).isoformat()
    elif isinstance(since_date, str):
        since_iso = since_date
    else:
        since_iso = None

    # 1) Kick off Apify run and fetch results
    run_id = _run_instagram_actor(instagram_username, since_iso)
    raw_items = _fetch_actor_results(run_id)

    # 2) Flatten posts from 'topPosts'/'latestPosts' or direct items
    posts = []
    for entry in raw_items:
        if entry.get("error"):
            # skip error entries
            continue
        # actor v3 returns nested topPosts/latestPosts
        if isinstance(entry.get("topPosts"), list) or isinstance(entry.get("latestPosts"), list):
            posts.extend(entry.get("topPosts", []))
            posts.extend(entry.get("latestPosts", []))
        # fallback on generic 'items' field
        elif isinstance(entry.get("items"), list):
            posts.extend(entry.get("items"))
        else:
            # assume entry itself is a post dict
            posts.append(entry)

    # 3) No-items guard
    if not posts:
        print(f"[WARN] No posts for @{instagram_username}")
        return

    # 4) Insert each post
    inserted = 0
    for it in posts:
        post_id       = it.get("id") or it.get("shortCode")
        caption       = it.get("description", "") or it.get("caption")
        author_handle = it.get("username") or it.get("ownerUsername")
        created_at    = it.get("publishedAt") or it.get("timestamp")
        like_count    = it.get("likesCount") or it.get("like_count") or 0
        comment_count = it.get("commentsCount") or it.get("comment_count") or 0
        video_url     = it.get("videoUrl") or it.get("video_url")
        image         = it.get("images", "")

        if not post_id or not created_at:
            continue

        insert_instagram_mention({
            "post_id":       post_id,
            "company_name":  company_name,
            "caption":       caption,
            "author_handle": author_handle,
            "created_at":    created_at,
            "like_count":    like_count,
            "comment_count": comment_count,
            "videourl":     video_url,
            "image":         image,
        })
        inserted += 1

    print(f"[INFO] Inserted {inserted} Instagram posts for '{company_name}' (@{instagram_username})")
