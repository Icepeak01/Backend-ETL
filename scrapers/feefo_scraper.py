# feefo_scraper.py
import time
import cloudscraper
from bs4 import BeautifulSoup
from datetime import datetime
from utils.db_helpers import insert_feefo_review
import logging

logger = logging.getLogger(__name__)

def fetch_feefo_page(
    company_name: str,
    feefo_slug: str,
    page_num: int,
) -> int:
    base_url = f"https://www.feefo.com/en-GB/reviews/{feefo_slug}"
    url = f"{base_url}?page={page_num}"
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )

    try:
        resp = scraper.get(url)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"[Feefo][{feefo_slug}] page {page_num} request failed: {e}")
        return 0

    soup = BeautifulSoup(resp.text, "html.parser")
    blocks = soup.find_all("div", {"data-aqa-id": "feedback-container"})
    if not blocks:
        logger.info(f"[Feefo][{feefo_slug}] no reviews on page {page_num}")
        return 0

    inserted = 0
    for b in blocks:
        data = {
            "company_name":      company_name,
            "feefo_business_info": feefo_slug,
            "customer_name":     None,
            "review_date":       None,
            "service_review":    None,
            "product_review":    None,
            "customer_location": None,
        }
        # customer name
        el = b.find("div", {"data-aqa-id": "customer-name"})
        data["customer_name"] = el.get_text(strip=True) if el else None

        # date DD/MM/YYYY
        el = b.find("div", {"data-aqa-id": "customer-purchased-date"})
        if el:
            txt = el.get_text(strip=True).replace("Date of purchase: ", "")
            try:
                data["review_date"] = datetime.strptime(txt, "%d/%m/%Y").date()
            except ValueError:
                data["review_date"] = None

        # service review
        el = b.find("div", {"data-aqa-id": "customer-comment-container"})
        data["service_review"] = el.get_text(strip=True) if el else None

        # product review
        wrapper = b.find("div", {"data-aqa-id": "feedback-product-container"})
        if wrapper:
            el = wrapper.find("div", {"data-aqa-id": "customer-comment-container"})
            data["product_review"] = el.get_text(strip=True) if el else None

        # location
        el = b.find("div", {"data-aqa-id": "customer-location"})
        data["customer_location"] = el.get_text(strip=True) if el else None

        try:
            insert_feefo_review(data)
            inserted += 1
        except Exception as e:
            logger.error(f"[Feefo][DB] insert error: {e}")

    # be polite to Feefo
    time.sleep(1.0)
    logger.info(f"[Feefo][{feefo_slug}] inserted {inserted} reviews from page {page_num}")
    return inserted
