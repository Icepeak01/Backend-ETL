# trustpilot_scraper.py
import random
import time
import logging

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from utils.db_helpers import insert_trustpilot_review
from utils.date_utils import parse_trustpilot_date

logger = logging.getLogger(__name__)

try:
    ua = UserAgent()
except Exception as e:
    logger.warning(f"fake_useragent init failed, falling back to static UA: {e}")
    ua = None

def _get_headers():
    try:
        ua_string = ua.random if ua else None
    except Exception as e:
        logger.warning(f"fake_useragent failed to generate UA, using static: {e}")
        ua_string = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        )
    return {
        "User-Agent": ua_string,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml",
    }

def fetch_trustpilot_page(company_name: str, company_web: str, page_num: int) -> int:
    base = f"https://uk.trustpilot.com/review/{company_web}"
    url = f"{base}?page={page_num}"
    headers = _get_headers()

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.warning(f"[Trustpilot][{company_web}][page {page_num}] request failed: {e}")
        return 0

    time.sleep(random.uniform(1.0, 3.0))
    soup = BeautifulSoup(resp.text, "html.parser")
    cards = soup.find_all("div", {"class": "styles_cardWrapper__g8amG styles_show__Z8n7u"})
    if not cards:
        logger.info(f"[Trustpilot][{company_web}] no reviews on page {page_num}")
        return 0

    count = 0
    for r in cards:
        data = {
            "company_name": company_name,
            "company_web_address": company_web,
            "author_name": None,
            "rating": None,
            "review_title": None,
            "review_body": None,
            "review_date": None,
        }
        # parse fields...
        name_el = r.find("div", {"class": "styles_consumerDetailsWrapper__4eZod"})
        if name_el:
            sp = name_el.find("span", {"class": "typography_heading-xs__osRhC"})
            if sp: data["author_name"] = sp.get_text(strip=True)

        rating_el = r.find("div", {"class": "star-rating_starRating__sdbkn"})
        if rating_el:
            img = rating_el.find("img")
            if img and img.has_attr("alt"):
                try:
                    data["rating"] = int(img["alt"].split()[1])
                except:
                    pass

        title = r.find("h2", {"class": "typography_heading-xs__osRhC"})
        if title: data["review_title"] = title.get_text(strip=True)

        body = r.find("p", {"class": "typography_body-l__v5JLj"})
        if body: data["review_body"] = body.get_text(strip=True)

        date_p = r.find("p", {"class": "typography_body-m__k2UI7"})
        if date_p:
            sp = date_p.find("span", {"class": "typography_body-m__k2UI7"})
            if sp:
                data["review_date"] = parse_trustpilot_date(sp.get_text(strip=True))

        try:
            insert_trustpilot_review(data)
            count += 1
        except Exception as e:
            logger.error(f"[Trustpilot][DB] insert error: {e}")

    logger.info(f"[Trustpilot][{company_web}] inserted {count} reviews from page {page_num}")
    return count
