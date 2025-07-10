# /scrapers/google_maps_reviews

import os
import time
import requests
from dotenv import load_dotenv
from utils.db_helpers import insert_google_maps_review

load_dotenv()
API_TOKEN   = os.getenv("APIFY_API_TOKEN")
ACTOR_ID    = os.getenv("APIFY_GOOGLE_MAPS_ACTOR", "compass~google-maps-reviews-scraper")
ACTS_BASE   = "https://api.apify.com/v2/acts"
DATASETS_BASE = "https://api.apify.com/v2/datasets"

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type":  "application/json"
}

def _run_actor(place_url, max_reviews=100, language="en"):
    endpoint = f"{ACTS_BASE}/{ACTOR_ID}/runs?token={API_TOKEN}"
    payload = {
        "startUrls": [{"url": place_url}],
        "maxReviews": max_reviews,
        "language": language
    }
    resp = requests.post(endpoint, headers=HEADERS, json=payload)
    resp.raise_for_status()
    return resp.json()["data"]["id"]

def _fetch_results(run_id):
    status_url = f"{ACTS_BASE}/{ACTOR_ID}/runs/{run_id}?token={API_TOKEN}"
    while True:
        resp = requests.get(status_url, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()["data"]
        status = data["status"]
        if status == "SUCCEEDED":
            break
        if status == "FAILED":
            raise RuntimeError(f"Actor run {run_id} failed: {data.get('error')}")
        time.sleep(2)

    dataset_id = data.get("defaultDatasetId")
    if not dataset_id:
        return []

    items_url = f"{DATASETS_BASE}/{dataset_id}/items?token={API_TOKEN}"
    items_resp = requests.get(items_url, headers=HEADERS)
    items_resp.raise_for_status()
    return items_resp.json()

def fetch_google_maps_reviews(company_name, place_url, max_reviews=100, language="en"):
    """
    Public: scrape reviews for a single Google Maps place URL.
    Returns number of reviews inserted.
    """
    try:
        run_id = _run_actor(place_url, max_reviews, language)
        items  = _fetch_results(run_id)
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            print(f"[WARN][GMAPS] no reviews for {company_name} @ {place_url}: {e}")
            return 0
        raise

    count = 0
    for item in items:
        review = {
            "company_name":   company_name,
            "place_url":      place_url,
            "reviewer_name":  item.get("name"),
            "rating":         item.get("stars"),
            "review_text":    item.get("text"),
            "review_date":    item.get("reviewDate"),
            "reviewUrl":     item.get("reviewUrl"),
            "owner_response": item.get("ownerResponse"),
        }
        insert_google_maps_review(review)
        count += 1

    print(f"[INFO] Inserted {count} Google Maps reviews for '{company_name}'")
    return count
