# task.py
import os
import time
from datetime import datetime, timedelta, timezone
import logging
from datetime import datetime, timedelta
from celery import Celery
from celery.schedules import crontab
from pytz import timezone as pytz_tz

from utils.db_helpers import (
    fetch_users_where_last_fetched_is_null,
    fetch_users_where_last_fetched_older_than,
    update_user_fetched,
    insert_twitter_mention,
    insert_instagram_mention,
    insert_feefo_review,
    insert_google_maps_review,
    insert_reddit_post,
    insert_facebook_post
)
from scrapers.twitter_scraper import fetch_tweets_for_company
from scrapers.instagram_scraper import fetch_instagram_for_company
from scrapers.trustpilot_scraper import fetch_trustpilot_page
from scrapers.feefo_scraper import fetch_feefo_page
from scrapers.google_maps_scraper import fetch_google_maps_reviews
from scrapers.twitter2_scraper import fetch_tweets_for_user
from scrapers.reddit_scraper import fetch_reddit_for_company
from scrapers.twitter3_scraper import fetch_tweets_sn
from scrapers.facebook_scraper import fetch_facebook_for_user

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
celery = Celery("etl_tasks", broker=redis_url, backend=redis_url)

# schedules from env
TP_HOURS = [int(h) for h in os.getenv("TP_GEN_SCHEDULE", "").split(",") if h]
TW_HOURS = [int(h) for h in os.getenv("TWITTER_GEN_SCHEDULE", "").split(",") if h]
IG_HOURS = [int(h) for h in os.getenv("IG_GEN_SCHEDULE", "").split(",") if h]
FEEFO_HOURS     = [int(h) for h in os.getenv("FEEFO_GEN_SCHEDULE", "").split(",") if h]
GMAPS_HOURS      = [int(h) for h in os.getenv("GMAPS_GEN_SCHEDULE", "").split(",") if h]
REDDIT_HOURS = [int(h) for h in os.getenv("REDDIT_GEN_SCHEDULE", "").split(",") if h]
FB_HOURS = [int(h) for h in os.getenv("FB_GEN_SCHEDULE", "").split(",") if h]







GMAPS_MAX_REVIEWS = int(os.getenv("GMAPS_MAX_REVIEWS", "100"))
FEEFO_MAX_PAGES = int(os.getenv("FEEFO_MAX_PAGES", "30"))
MAX_TP = int(os.getenv("TP_MAX_PAGES", "30"))
REDDIT_MAX_POSTS = int(os.getenv("REDDIT_MAX_POSTS", "30"))
TWITTER3_MAX_TWEETS = int(os.getenv("TWITTER3_MAX_TWEETS", "100"))
FB_MAX_POSTS = int(os.getenv("FB_MAX_POSTS", "100"))
CATCHUP_CRON = os.getenv("CATCHUP_CRON", "*/5")
CATCHUP_DT   = lambda: datetime.now(timezone.utc) - timedelta(days=30*4)
LOCAL_TZ = pytz_tz(os.getenv("LOCAL_TIMEZONE", "UTC"))

celery.conf.beat_schedule = {
   # "tp-general": {"task": "tasks.tp_general",  "schedule": crontab(minute=0, hour=TP_HOURS)},
   # "tp-catchup":{"task": "tasks.tp_catchup", "schedule": crontab(minute=CATCHUP_CRON)},
    #"tw-general": {"task": "tasks.tw_general",  "schedule": crontab(minute=0, hour=TW_HOURS)},
    #"tw-catchup":{"task": "tasks.tw_catchup", "schedule": crontab(minute=CATCHUP_CRON)},
    #"ig-general": {"task": "tasks.ig_general",  "schedule": crontab(minute=0, hour=IG_HOURS)},
    #"ig-catchup":{"task": "tasks.ig_catchup", "schedule": crontab(minute=CATCHUP_CRON)},
  #  "feefo-general": {"task": "tasks.feefo_general", "schedule": crontab(minute=0, hour=FEEFO_HOURS)},
   # "feefo-catchup": {"task": "tasks.feefo_catchup", "schedule": crontab(minute=CATCHUP_CRON)},
    #"gmaps-general": {"task": "tasks.gmaps_general", "schedule": crontab(minute=0, hour=GMAPS_HOURS)},
   # "gmaps-catchup": {"task": "tasks.gmaps_catchup", "schedule": crontab(minute=CATCHUP_CRON)},
    #"tw2-general": {"task": "tasks.tw2_general", "schedule": crontab(minute=0, hour=TW_HOURS)},
    #"tw2-catchup": {"task": "tasks.tw2_catchup", "schedule": crontab(minute=CATCHUP_CRON)},
    #"reddit-general": {"task": "tasks.reddit_general", "schedule": crontab(minute=0, hour=REDDIT_HOURS)},
   # "reddit-catchup": {"task": "tasks.reddit_catchup", "schedule": crontab(minute=CATCHUP_CRON)},
    #"tw3-general": {"task": "tasks.tw3_general", "schedule": crontab(minute=0, hour=TW_HOURS)},
    #"tw3-catchup": {"task": "tasks.tw3_catchup", "schedule": crontab(minute=CATCHUP_CRON)},
    "fb-general": {"task": "tasks.fb_general", "schedule": crontab(minute=0, hour=FB_HOURS)},
    "fb-catchup": {"task": "tasks.fb_catchup", "schedule": crontab(minute=CATCHUP_CRON)},
}

def _cutoff(minutes=40):
    return datetime.now(LOCAL_TZ) - timedelta(minutes=minutes)

# — Trustpilot General —
@celery.task(bind=True, max_retries=3, name="tasks.tp_general")
def tp_general(self):
    users = fetch_users_where_last_fetched_older_than("trustpilot", _cutoff())
    for u in users:
        tp_scrape_general.delay(u["id"], u["company_name"], u["company_web_address"])

# — Trustpilot Catchup —
@celery.task(bind=True, max_retries=3, name="tasks.tp_catchup")
def tp_catchup(self):
    users = fetch_users_where_last_fetched_is_null("trustpilot")
    for u in users:
        tp_scrape_catchup.delay(u["id"], u["company_name"], u["company_web_address"])


@celery.task(bind=True, name="tasks.tp_scrape_general")
def tp_scrape_general(self, uid, name, web):
    if not web:
        logger.warning(f"[TP] skip user {uid} (no web)")
        return
    total = 0
    for p in range(1, 4):
        try:
            total += fetch_trustpilot_page(name, web, p)
        except Exception as e:
            logger.error(f"[TP][{uid}] page {p} failed: {e}")
            self.retry(exc=e, countdown=60*p)
    if total:
        update_user_fetched(uid, "trustpilot")
        logger.info(f"[TP] general scraped {total} for {uid}")


@celery.task(bind=True, name="tasks.tp_scrape_catchup")
def tp_scrape_catchup(self, uid, name, web):
    if not web:
        logger.warning(f"[TP] skip user {uid} (no web)")
        return
    total = 0
    for p in range(1, MAX_TP+1):
        try:
            total += fetch_trustpilot_page(name, web, p)
        except Exception as e:
            logger.error(f"[TP][{uid}] page {p} failed: {e}")
            self.retry(exc=e, countdown=60*p)
    if total:
        update_user_fetched(uid, "trustpilot")
        logger.info(f"[TP] catchup scraped {total} for {uid}")



# — Twitter General & Catchup —
@celery.task(bind=True, name="tasks.tw_general")
def tw_general(self):
    users = fetch_users_where_last_fetched_older_than("twitter", _cutoff())
    for u in users:
        tw_scrape_general.delay(u["id"], u["company_name"])

@celery.task(bind=True, name="tasks.tw_catchup")
def tw_catchup(self):
    users = fetch_users_where_last_fetched_is_null("twitter")
    for u in users:
        tw_scrape_catchup.delay(u["id"], u["company_name"])

@celery.task(bind=True, max_retries=3, name="tasks.tw_scrape_general")
def tw_scrape_general(self, uid, name):
    tweets = fetch_tweets_for_company(name, since_id=None)
    for t in tweets: insert_twitter_mention(t)
    if tweets:
        update_user_fetched(uid, "twitter")
        logger.info(f"[TW] scraped {len(tweets)} for {uid}")

@celery.task(bind=True, max_retries=3, name="tasks.tw_scrape_catchup")
def tw_scrape_catchup(self, uid, name):
    tweets = fetch_tweets_for_company(name, since_id=None)
    for t in tweets: insert_twitter_mention(t)
    if tweets:
        update_user_fetched(uid, "twitter")
        logger.info(f"[TW] catchup scraped {len(tweets)} for {uid}")

# — Instagram General & Catchup —
@celery.task(bind=True, name="tasks.ig_general")
def ig_general(self):
    cutoff = datetime.now(LOCAL_TZ) - timedelta(minutes=40)
    users = fetch_users_where_last_fetched_older_than("instagram", cutoff)
    for u in users:
        ig_scrape_general.delay(
            user_id=u["id"],
            company_name=u["company_name"],
            instagram_username=u["instagram_username"],
            last_ts=u["last_fetched_instagram"],
        )

@celery.task(bind=True, name="tasks.ig_catchup")
def ig_catchup(self):
    users = fetch_users_where_last_fetched_is_null("instagram")
    for u in users:
        ig_scrape_catchup.delay(
            user_id=u["id"],
            company_name=u["company_name"],
            instagram_username=u["instagram_username"],
        )

@celery.task(bind=True, max_retries=3, name="tasks.ig_scrape_general")
def ig_scrape_general(self, user_id, company_name, instagram_username, last_ts):
    if not instagram_username:
        logger.warning(f"[IG] skip user {user_id} (no instagram_username)")
        return

    # normalize last_ts → ISO
    since_iso = last_ts.astimezone(LOCAL_TZ).isoformat() if last_ts else None

    try:
        fetch_instagram_for_company(
            company_name=company_name,
            instagram_username=instagram_username,
            since_date=since_iso,
        )
        update_user_fetched(user_id, "instagram")
        logger.info(f"[IG] general scraped for user {user_id}")
    except Exception as e:
        logger.error(f"[IG][{user_id}] general scrape failed: {e}")
        self.retry(exc=e, countdown=300)

@celery.task(bind=True, max_retries=3, name="tasks.ig_scrape_catchup")
def ig_scrape_catchup(self, user_id, company_name, instagram_username):
    if not instagram_username:
        logger.warning(f"[IG] skip user {user_id} (no instagram_username)")
        return

    try:
        fetch_instagram_for_company(
            company_name=company_name,
            instagram_username=instagram_username,
            since_date=None,
        )
        update_user_fetched(user_id, "instagram")
        logger.info(f"[IG] catchup scraped for user {user_id}")
    except Exception as e:
        logger.error(f"[IG][{user_id}] catchup scrape failed: {e}")
        self.retry(exc=e, countdown=300)




# — Feefo General & Catchup —
@celery.task(bind=True, max_retries=3, name="tasks.feefo_general")
def feefo_general(self):
    users = fetch_users_where_last_fetched_older_than("feefo", _cutoff())
    for u in users:
        feefo_scrape_general.delay(
            u["id"],
            u["company_name"],
            u.get("feefo_business_info"),  # slug from users.feefo_business_info
        )



@celery.task(bind=True, max_retries=3, name="tasks.feefo_catchup")
def feefo_catchup(self):
    users = fetch_users_where_last_fetched_is_null("feefo")
    for u in users:
        feefo_scrape_catchup.delay(
            u["id"],
            u["company_name"],
            u.get("feefo_business_info"),
        )



@celery.task(bind=True, max_retries=3, name="tasks.feefo_scrape_general")
def feefo_scrape_general(self, uid, company_name, feefo_slug):
    if not feefo_slug:
        logger.warning(f"[Feefo] skip user {uid} (no feefo_business_info)")
        return
    total = 0
    try:
        for page in range(1, 4):
            total += fetch_feefo_page(company_name, feefo_slug, page)
        if total:
            update_user_fetched(uid, "feefo")
        logger.info(f"[Feefo] general scraped {total} for {uid}")
    except Exception as e:
        logger.error(f"[Feefo][{uid}] general scrape failed: {e}")
        self.retry(exc=e, countdown=300)




@celery.task(bind=True, max_retries=3, name="tasks.feefo_scrape_catchup")
def feefo_scrape_catchup(self, uid, company_name, feefo_slug):
    if not feefo_slug:
        logger.warning(f"[Feefo] skip user {uid} (no feefo_business_info)")
        return
    total = 0
    try:
        for page in range(1, FEEFO_MAX_PAGES + 1):
            total += fetch_feefo_page(company_name, feefo_slug, page)
        if total:
            update_user_fetched(uid, "feefo")
        logger.info(f"[Feefo] catchup scraped {total} for {uid}")
    except Exception as e:
        logger.error(f"[Feefo][{uid}] catchup scrape failed: {e}")
        self.retry(exc=e, countdown=300)


# — Google Maps General & Catchup —

@celery.task(bind=True, max_retries=3, name="tasks.gmaps_general")
def gmaps_general(self):
    users = fetch_users_where_last_fetched_older_than("google_maps", _cutoff())
    for u in users:
        gmaps_scrape_general.delay(
            u["id"],
            u["company_name"],
            u["place_url"],
        )


@celery.task(bind=True, max_retries=3, name="tasks.gmaps_catchup")
def gmaps_catchup(self):
    users = fetch_users_where_last_fetched_is_null("google_maps")
    for u in users:
        gmaps_scrape_catchup.delay(
            u["id"],
            u["company_name"],
            u["place_url"],
        )



@celery.task(bind=True, max_retries=3, name="tasks.gmaps_scrape_general")
def gmaps_scrape_general(self, uid, company_name, place_url):
    if not place_url:
        logger.warning(f"[GMAPS] skip user {uid} (missing place_url)")
        return
    try:
        count = fetch_google_maps_reviews(
            company_name=company_name,
            place_url=place_url,
            max_reviews=GMAPS_MAX_REVIEWS,
        )
        if count:
            update_user_fetched(uid, "google_maps")
            logger.info(f"[Google Maps] general scraped {count} reviews for {uid}")
    except Exception as e:
        logger.error(f"[Google Maps][{uid}] catchup scrape failed: {e}")
        self.retry(exc=e, countdown=300)

@celery.task(bind=True, max_retries=3, name="tasks.gmaps_scrape_catchup")
def gmaps_scrape_catchup(self, uid, company_name, place_url):
    if not place_url:
        logger.warning(f"[GMAPS] skip user {uid} (missing place_id/url)")
        return
    try:
        count = fetch_google_maps_reviews(
            company_name=company_name,
            place_url=place_url,
            max_reviews=GMAPS_MAX_REVIEWS,
        )
        if count:
            update_user_fetched(uid, "google_maps")
            logger.info(f"[Google Maps] general scraped {count} reviews for {uid}")
    except Exception as e:
        logger.error(f"[Google Maps][{uid}] catchup scrape failed: {e}")
        self.retry(exc=e, countdown=300)



# --- New Twitter-2 tasks using Apify actor ---
@celery.task(bind=True, name="tasks.tw2_general")
def tw2_general(self):
    
    users = fetch_users_where_last_fetched_older_than("twitter2", _cutoff())
    for u in users:
        tw2_scrape_general.delay(
            uid=u["id"],
            company_name=u["company_name"],
            twitter_username=u["twitter_username"],
            last_fetched=u["last_fetched_twitter2"],
        )

@celery.task(bind=True, name="tasks.tw2_catchup")
def tw2_catchup(self):
    users = fetch_users_where_last_fetched_is_null("twitter2")
    for u in users:
        tw2_scrape_catchup.delay(
            uid=u["id"],
            company_name=u["company_name"],
            twitter_username=u["twitter_username"],
        )

@celery.task(bind=True, max_retries=3, name="tasks.tw2_scrape_general")
def tw2_scrape_general(self, uid, company_name, twitter_username, last_fetched):
    """
    Incremental scrape of up to 100 tweets by @twitter_username since last_fetched.
    """
    if not twitter_username:
        logger.warning(f"[TW2] skip user {uid} (no twitter_username)")
        return

    try:
        count = fetch_tweets_for_user(
            company_name=company_name,
            twitter_username=twitter_username,
            last_fetched=last_fetched,
            max_items=100
        )
        if count:
            update_user_fetched(uid, "twitter2")
            logger.info(f"[TW2] scraped {count} tweets for {uid}")
    except Exception as e:
        logger.error(f"[TW2][{uid}] general scrape failed: {e}")
        raise self.retry(exc=e, countdown=300)

@celery.task(bind=True, max_retries=3, name="tasks.tw2_scrape_catchup")
def tw2_scrape_catchup(self, uid, company_name, twitter_username):
    """
    Full catchup scrape of up to 100 tweets by @twitter_username.
    """
    if not twitter_username:
        logger.warning(f"[TW2] skip user {uid} (no twitter_username)")
        return

    try:
        count = fetch_tweets_for_user(
            company_name=company_name,
            twitter_username=twitter_username,
            last_fetched=None,
            max_items=100
        )
        if count:
            update_user_fetched(uid, "twitter2")
            logger.info(f"[TW2] catchup scraped {count} tweets for {uid}")
    except Exception as e:
        logger.error(f"[TW2][{uid}] catchup scrape failed: {e}")
        raise self.retry(exc=e, countdown=300)



# --- Reddit tasks ---
@celery.task(bind=True, max_retries=3, name="tasks.reddit_general")
def reddit_general(self):
    users = fetch_users_where_last_fetched_older_than("reddit", _cutoff())
    for u in users:
        reddit_scrape_general.delay(
            uid=u["id"],
            company_name=u["company_name"],
            company_web=u.get("company_web_address"),
        )

@celery.task(bind=True, max_retries=3, name="tasks.reddit_catchup")
def reddit_catchup(self):
    users = fetch_users_where_last_fetched_is_null("reddit")
    for u in users:
        reddit_scrape_catchup.delay(
            uid=u["id"],
            company_name=u["company_name"],
            company_web=u.get("company_web_address"),
        )

@celery.task(bind=True, max_retries=3, name="tasks.reddit_scrape_general")
def reddit_scrape_general(self, uid, company_name, company_web):
    if not company_web:
        logger.warning(f"[Reddit] skip user {uid} (no company_web_address)")
        return 0

    try:
        n = fetch_reddit_for_company(company_name, company_web)
        if n:
            update_user_fetched(uid, "reddit")
        logger.info(f"[Reddit] general scraped {n} posts for user {uid}")
        return n
    except Exception as e:
        logger.error(f"[Reddit][{uid}] general scrape failed: {e}")
        raise self.retry(exc=e, countdown=300)

@celery.task(bind=True, max_retries=3, name="tasks.reddit_scrape_catchup")
def reddit_scrape_catchup(self, uid, company_name, company_web):
    if not company_web:
        logger.warning(f"[Reddit] skip user {uid} (no company_web_address)")
        return 0

    try:
        n = fetch_reddit_for_company(company_name, company_web)
        if n:
            update_user_fetched(uid, "reddit")
        logger.info(f"[Reddit] catch-up scraped {n} posts for user {uid}")
        return n
    except Exception as e:
        logger.error(f"[Reddit][{uid}] catch-up scrape failed: {e}")
        raise self.retry(exc=e, countdown=300)





# — Twitter3 General & Catchup —

@celery.task(bind=True, name="tasks.tw3_general")
def tw3_general(self):
    """
    Incremental scrape: from last_fetched_twitter3 until now.
    """
    cutoff = _cutoff()
    users = fetch_users_where_last_fetched_older_than("twitter3", cutoff)
    for u in users:
        tw3_scrape_general.delay(
            u["id"],
            u["company_name"],
            u.get("twitter_username"),
            u.get("last_fetched_twitter3"),
        )

@celery.task(bind=True, name="tasks.tw3_catchup")
def tw3_catchup(self):
    """
    One‐off catchup for brand‐new users: last four months.
    """
    users = fetch_users_where_last_fetched_is_null("twitter3")
    for u in users:
        tw3_scrape_catchup.delay(
            u["id"],
            u["company_name"],
            u.get("twitter_username"),
        )

@celery.task(bind=True, max_retries=3, name="tasks.tw3_scrape_general")
def tw3_scrape_general(self, uid, company_name, twitter_username, last_fetched):
    if not twitter_username:
        logger.warning(f"[TW3] skip user {uid} (no twitter_username)")
        return 0

    try:
        # pass `since=last_fetched`; max_tweets will default
        count = fetch_tweets_sn(
            company_name,
            twitter_username,
            since=last_fetched
        )
        if count:
            update_user_fetched(uid, "twitter3")
        logger.info(f"[TW3] general scraped {count} tweets for {uid}")
        return count
    except Exception as e:
        logger.error(f"[TW3][{uid}] general scrape failed: {e}")
        raise self.retry(exc=e, countdown=300)

@celery.task(bind=True, max_retries=3, name="tasks.tw3_scrape_catchup")
def tw3_scrape_catchup(self, uid, company_name, twitter_username):
    if not twitter_username:
        logger.warning(f"[TW3] skip user {uid} (no twitter_username)")
        return 0

    try:
        # pass since=None so scraper uses default 4-month window
        count = fetch_tweets_sn(
            company_name,
            twitter_username,
            since=None
        )
        if count:
            update_user_fetched(uid, "twitter3")
        logger.info(f"[TW3] catchup scraped {count} tweets for {uid}")
        return count
    except Exception as e:
        logger.error(f"[TW3][{uid}] catchup scrape failed: {e}")
        raise self.retry(exc=e, countdown=300)


# --- Facebook General & Catchup ---
@celery.task(bind=True, name="tasks.fb_general")
def fb_general(self):
    users = fetch_users_where_last_fetched_older_than("facebook", CATCHUP_DT())
    for u in users:
        fb_scrape_general.delay(
          uid=u["id"],
          company_name=u["company_name"],
          fb_username=u.get("facebook_username"),
          last_fetched=u.get("last_fetched_facebook")
        )

@celery.task(bind=True, name="tasks.fb_catchup")
def fb_catchup(self):
    users = fetch_users_where_last_fetched_is_null("facebook")
    for u in users:
        fb_scrape_catchup.delay(
          uid=u["id"],
          company_name=u["company_name"],
          fb_username=u.get("facebook_username")
        )

@celery.task(bind=True, max_retries=3, name="tasks.fb_scrape_general")
def fb_scrape_general(self, uid, company_name, fb_username, last_fetched):
    if not fb_username:
        logger.warning(f"[FB] skip user {uid} (no facebook_username)")
        return
    try:
        cnt = fetch_facebook_for_user(company_name, fb_username, since_dt=last_fetched)
        if cnt:
            update_user_fetched(uid, "facebook")
            logger.info(f"[FB] general scraped {cnt} posts for {uid}")
    except Exception as e:
        logger.error(f"[FB][{uid}] general scrape failed: {e}")
        raise self.retry(exc=e, countdown=300)

@celery.task(bind=True, max_retries=3, name="tasks.fb_scrape_catchup")
def fb_scrape_catchup(self, uid, company_name, fb_username):
    if not fb_username:
        logger.warning(f"[FB] skip user {uid} (no facebook_username)")
        return
    try:
        cnt = fetch_facebook_for_user(company_name, fb_username, since_dt=None)
        if cnt:
            update_user_fetched(uid, "facebook")
            logger.info(f"[FB] catchup scraped {cnt} posts for {uid}")
    except Exception as e:
        logger.error(f"[FB][{uid}] catchup scrape failed: {e}")
        raise self.retry(exc=e, countdown=300)