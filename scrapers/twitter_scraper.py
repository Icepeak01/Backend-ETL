# scrapers/twitter_scraper.py
import os
import requests
from datetime import datetime, timezone

TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

def fetch_tweets_for_company(company_name, since_id=None):
    headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
    url = "https://api.twitter.com/2/tweets/search/recent"
    params = {
        "query": company_name,
        "max_results": 10,
        "tweet.fields": "created_at,author_id,public_metrics",
    }
    if since_id:
        params["since_id"] = since_id

    all_tweets = []
    next_token = None
    while True:
        if next_token:
            params["next_token"] = next_token
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        all_tweets.extend(data.get("data", []))
        meta = data.get("meta", {})
        next_token = meta.get("next_token")
        if not next_token or len(all_tweets) >= 100:
            break
    # transform into your insert format
    tweets = []
    for item in all_tweets:
        tweets.append({
            "tweet_id": item["id"],
            "company_name": company_name,
            "text": item["text"],
            "author_handle": item["author_id"],
            "created_at": item["created_at"],
            "reply_count": item["public_metrics"].get("reply_count", 0),
            "like_count": item["public_metrics"].get("like_count", 0)
        })
    return tweets

