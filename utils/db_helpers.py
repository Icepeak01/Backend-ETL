# utils/db_helpers.py

import os
import time
import logging
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

DB_PARAMS = dict(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname=os.getenv("DB_NAME"),
    sslmode=os.getenv("DB_SSLMODE", "require"),
)

# Start with no pool; each worker process will initialize its own
_pool = None

def _init_pool():
    global _pool
    if _pool is None:
        # will only run once, per process
        _pool = ThreadedConnectionPool(minconn=1, maxconn=50, **DB_PARAMS)
    return _pool

def _get_pooled_conn(retries=3, backoff=1.0):
    last_exc = None
    pool = _init_pool()
    for i in range(1, retries+1):
        try:
            return pool.getconn()
        except OperationalError as e:
            last_exc = e
            logger.warning(f"DB connection attempt {i}/{retries} failed: {e}")
            time.sleep(backoff)
    raise last_exc

def _release_pooled_conn(conn):
    _init_pool().putconn(conn)

def fetch_users_where_last_fetched_is_null(platform: str):
    col = f"last_fetched_{platform}"
    conn = _get_pooled_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"""
                SELECT id, company_name, company_web_address, instagram_username, twitter_username, feefo_business_info, place_url, last_fetched_twitter3, facebook_username, last_fetched_facebook, {col}
                  FROM users
                 WHERE {col} IS NULL
            """)
            return cur.fetchall()
    finally:
        _release_pooled_conn(conn)


def fetch_users_where_last_fetched_older_than(platform: str, cutoff_time):
    col = f"last_fetched_{platform}"
    conn = _get_pooled_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"""
                SELECT id, company_name, company_web_address, instagram_username, twitter_username, feefo_business_info, place_url, last_fetched_twitter3, facebook_username, last_fetched_facebook, {col}
                  FROM users
                 WHERE {col} < %s
            """, (cutoff_time,))
            return cur.fetchall()
    finally:
        _release_pooled_conn(conn)

def update_user_fetched(user_id: int, platform: str):
    col = f"last_fetched_{platform}"
    conn = _get_pooled_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE users
                   SET {col} = NOW()
                 WHERE id = %s
            """, (user_id,))
            conn.commit()
    finally:
        _release_pooled_conn(conn)

def insert_twitter_mention(tweet: dict):
    conn = _get_pooled_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO twitter_mentions
                  (tweet_id, company_name, text, author_handle, created_at, reply_count, fetched_at)
                VALUES
                  (%(tweet_id)s, %(company_name)s, %(text)s, %(author_handle)s,
                   %(created_at)s, %(reply_count)s, NOW())
                ON CONFLICT (tweet_id) DO NOTHING;
            """, tweet)
            conn.commit()
    except OperationalError as e:
        logger.error(f"[Twitter][DB] insert failed: {e}")
    finally:
        _release_pooled_conn(conn)

def insert_instagram_mention(post: dict):
    conn = _get_pooled_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO instagram_mentions
                  (post_id, company_name, caption, author_handle, created_at,
                   like_count, comment_count, fetched_at, image, videourl)
                VALUES
                  (%(post_id)s, %(company_name)s, %(caption)s,
                   %(author_handle)s, %(created_at)s, %(like_count)s,
                   %(comment_count)s, NOW(), %(image)s, %(videourl)s)
                ON CONFLICT (post_id) DO NOTHING;
            """, post)
            conn.commit()
    except OperationalError as e:
        logger.error(f"[Instagram][DB] insert failed: {e}")
    finally:
        _release_pooled_conn(conn)

def insert_trustpilot_review(review: dict):
    conn = _get_pooled_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trustpilot_reviews
                  (company_name, company_web_address, author_name,
                   rating, review_title, review_body, review_date, fetched_at)
                VALUES
                  (%(company_name)s, %(company_web_address)s,
                   %(author_name)s, %(rating)s, %(review_title)s,
                   %(review_body)s, %(review_date)s, NOW())
                ON CONFLICT (company_name, author_name, review_title, review_date)
                DO NOTHING;
            """, review)
            conn.commit()
    except OperationalError as e:
        logger.error(f"[Trustpilot][DB] insert failed: {e}")
    finally:
        _release_pooled_conn(conn)


def insert_feefo_review(review: dict):
    """
    review keys: company_name, feefo_business_info, customer_name,
                 service_review, product_review, customer_location, review_date (DATE)
    """
    conn = _get_pooled_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO feefo_reviews
                  (company_name, feefo_business_info,
                   customer_name, service_review, product_review,
                   customer_location, review_date, fetched_at)
                VALUES
                  (%(company_name)s, %(feefo_business_info)s,
                   %(customer_name)s, %(service_review)s, %(product_review)s,
                   %(customer_location)s, %(review_date)s, NOW())
                ON CONFLICT (company_name, feefo_business_info,
                             customer_name, service_review, review_date)
                DO NOTHING;
            """, review)
            conn.commit()
    finally:
        _release_pooled_conn(conn)




def insert_google_maps_review(review: dict):
    """
    review keys:
      company_name, place_id, place_url,
      reviewer_name, rating, review_text,
      review_date (ISO), review_url, owner_response
    """
    conn = _get_pooled_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO google_maps_reviews
                  (company_name, place_url,
                   reviewer_name, rating, review_text,
                   review_date, reviewUrl, owner_response, fetched_at)
                VALUES
                  (%(company_name)s, %(place_url)s,
                   %(reviewer_name)s, %(rating)s, %(review_text)s,
                   %(review_date)s, %(reviewUrl)s, %(owner_response)s, NOW())
                ON CONFLICT (place_url, reviewer_name, reviewUrl)
                DO NOTHING;
            """, review)
            conn.commit()
    except OperationalError as e:
        logger.error(f"[GoogleMaps][DB] insert failed: {e}")
    finally:
        _release_pooled_conn(conn)



# Insert Reddit
def insert_reddit_post(post: dict):
    """
    Insert one Reddit post into reddit_posts, skipping duplicates on (company_name, post_url).
    Expects keys:
      - company_name
      - post_url
      - title
      - author
      - image_url
      - votes
      - comments
      - full_review
      - review_date
    """
    conn = _get_pooled_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reddit_posts
                  (company_name, post_url, title, author, image_url,
                   votes, comments, full_review, review_date, fetched_at)
                VALUES (
                  %(company_name)s,
                  %(post_url)s,
                  %(title)s,
                  %(author)s,
                  %(image_url)s,
                  %(votes)s,
                  %(comments)s,
                  %(full_review)s,
                  %(review_date)s,
                  NOW()
                )
                ON CONFLICT (company_name, post_url, review_date) DO NOTHING;
            """, post)
            conn.commit()
    finally:
        conn.close()

# Insert Facebook

def insert_facebook_post(post: dict):
    """
    Expects keys:
      post_id, company_name, facebook_username, message,
      created_at, reactions_count, comments_count, share_count, post_url
    """
    conn = _get_pooled_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO facebook_posts
                  (post_id, company_name, facebook_username, message,
                   created_at, reactions_count, comments_count,
                   share_count, post_url, fetched_at)
                VALUES
                  (%(post_id)s, %(company_name)s, %(facebook_username)s, %(message)s,
                   %(created_at)s, %(reactions_count)s, %(comments_count)s,
                   %(share_count)s, %(post_url)s, NOW())
                ON CONFLICT (post_id) DO NOTHING;
            """, post)
            conn.commit()
    finally:
        _release_pooled_conn(conn)
