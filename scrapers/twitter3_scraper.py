# scrapers/twitter3_scraper.py
import os
import ssl
import time
import logging
from datetime import datetime, timedelta
from itertools import islice
from dotenv import load_dotenv
from snscrape.modules.twitter import TwitterUserScraper
from utils.db_helpers import insert_twitter_mention
import certifi

# —————— SSL / CA bundle setup ——————
# Let both the requests library and Python’s ssl module trust certifi’s bundle
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"]       = certifi.where()
# As a last‐resort fallback, disable verification entirely for snscrape’s HTTP calls:
ssl._create_default_https_context = ssl._create_unverified_context

# —————— Config & logger ——————
load_dotenv()
logger = logging.getLogger(__name__)
MAX_TWEETS     = int(os.getenv("TWITTER3_MAX_TWEETS", "100"))
CATCHUP_MONTHS = 4
THROTTLE_SEC   = float(os.getenv("TWITTER3_THROTTLE_SEC", "1.0"))

def fetch_tweets_sn(company_name: str,
                    twitter_username: str,
                    since: datetime = None,
                    max_tweets: int = MAX_TWEETS) -> int:
    """
    Scrape a user’s timeline with snscrape.TwitterUserScraper,
    skipping tweets older than `since`, throttling each request.
    """
    if not since:
        since = datetime.utcnow() - timedelta(days=30 * CATCHUP_MONTHS)

    logger.info(f"[TW3] scraping @{twitter_username} timeline since {since.isoformat()}")
    scraper = TwitterUserScraper(twitter_username)
    count = 0

    for tweet in islice(scraper.get_items(), None):
        # stop once we hit older tweets
        if tweet.date < since:
            break

        rec = {
            "tweet_id":      str(tweet.id),
            "company_name":  company_name,
            "text":          tweet.content,
            "author_handle": tweet.user.username,
            "created_at":    tweet.date.isoformat(),
            "reply_count":   getattr(tweet, "replyCount", 0) or 0,
            "like_count":    getattr(tweet, "likeCount", 0) or 0,
        }

        try:
            insert_twitter_mention(rec)
            count += 1
        except Exception as db_e:
            logger.error(f"[TW3][DB] failed insert {tweet.id}: {db_e}")

        # throttle to avoid blocks
        time.sleep(THROTTLE_SEC)
        if count >= max_tweets:
            break

    logger.info(f"[TW3] inserted {count} tweets for @{twitter_username}")
    return count
